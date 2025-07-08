@Listeners(TestResultListener.class)
public class BaseTest {
    protected static SessionFactory greenplumSessionFactory;
    protected static SessionFactory hadoopSessionFactory;

    // Greenplum thread-local переменные
    private static final ThreadLocal<Subject> greenplumKerberosTicket = new ThreadLocal<>();
    protected static final ThreadLocal<Session> greenplumSession = new ThreadLocal<>();
    protected static final ThreadLocal<Transaction> greenplumTransaction = new ThreadLocal<>();
    protected static final ThreadLocal<BaseDaoGreenplum> baseDaoGreenplum = new ThreadLocal<>();

    // Hadoop thread-local переменные
    protected static final ThreadLocal<Session> hadoopSession = new ThreadLocal<>();
    protected static final ThreadLocal<Transaction> hadoopTransaction = new ThreadLocal<>();
    protected static final ThreadLocal<BaseDaoHadoop> baseDaoHadoop = new ThreadLocal<>();

    protected static final ThreadLocal<TestUtil> testUtil = new ThreadLocal<>();
    protected SoftAssert softly;

    protected static Properties credentials;
    protected static Properties settings;

    @BeforeSuite
    @Step("Инициализация")
    protected void setUpSuite(ITestContext context) {
        settings = getSettingsProperties();
        credentials = getCredentialsProperties();

        setupKerberosCredentials(credentials);

        if (settings.getProperty("connectToGreenplum").equals("true"))
            Subject.doAs(loginKerberosForGreenplum(credentials), (PrivilegedAction<SessionFactory>) () -> {
                createGreenplumSessionFactory();
                return null;
            });

        if (settings.getProperty("connectToHadoop").equals("true"))
            Subject.doAs(loginKerberosForHadoop(credentials), (PrivilegedAction<SessionFactory>) () -> {
                createHadoopSessionFactory();
                return null;
            });

        installSpecToCTL(settings, credentials);

        if (settings.getProperty("useZephyr").equals("true")) {
            loginToJira(credentials, settings);
        }

        stream(context.getAllTestMethods()).forEach(method -> method.setRetryAnalyzerClass(RestartFailedTests.class));
    }

    @BeforeClass
    public void createUniqueGreenplumKerberosTicket() {
        greenplumKerberosTicket.set(loginKerberosForGreenplum(credentials));
    }

    @BeforeMethod(alwaysRun = true)
    public void setupSession() {
        try {
            Subject.doAs(greenplumKerberosTicket.get(), (PrivilegedAction<Object>) () -> {
                if (settings.getProperty("connectToGreenplum").equals("true")) {
                    Session gpSession = greenplumSessionFactory.openSession();
                    greenplumSession.set(gpSession);
                    getLog(BaseTest.class).info("Greenplum session opened: {}", gpSession.hashCode());

                    if (gpSession == null || !gpSession.isOpen()) {
                        throw new RuntimeException("Greenplum session is null or not open");
                    }

                    Transaction gpTransaction = gpSession.beginTransaction();
                    greenplumTransaction.set(gpTransaction);

                    getLog(BaseTest.class).info("Greenplum transaction {} started for session {}",
                            gpTransaction.hashCode(), gpSession.hashCode());

                    baseDaoGreenplum.set(new BaseDaoGreenplumImpl(gpSession, gpTransaction));
                }

                if (settings.getProperty("connectToHadoop").equals("true")) {
                    Session hdSession = hadoopSessionFactory.openSession();
                    hadoopSession.set(hdSession);

                    Transaction hdTransaction = hdSession.beginTransaction();
                    hadoopTransaction.set(hdTransaction);

                    baseDaoHadoop.set(new BaseDaoHadoopImpl(hdSession));
                    getLog(BaseTest.class).info("Hadoop transaction started: {}", hdTransaction.hashCode());
                }

                softly = new SoftAssert();

                testUtil.set(new TestUtil(
                        greenplumSession.get(),
                        greenplumTransaction.get(),
                        softly,
                        baseDaoGreenplum.get(),
                        baseDaoHadoop.get()));

                return null;
            });
        } catch (IllegalStateException | HibernateException e) {
            getLog(BaseTest.class).error("Тест пропущен, т.к. не удалось создать подключение к БД", e);
            throw new SkipException("Тест пропущен, т.к. не удалось создать подключение к БД");
        }
    }

    @AfterMethod(alwaysRun = true)
    @Step("Закрытие Session")
    public void closeSession() {
        try {
            if (greenplumTransaction.get() != null && greenplumTransaction.get().isActive()) {
                greenplumTransaction.get().commit();
                getLog(BaseTest.class).info("Greenplum transaction commit {}", greenplumTransaction.get().hashCode());
            }
        } catch (Exception e) {
            if (greenplumTransaction.get() != null && greenplumTransaction.get().isActive()) {
                greenplumTransaction.get().rollback();
            }
            getLog(BaseTest.class).error("Ошибка при коммите транзакции Greenplum", e);
        } finally {
            Session gpSession = greenplumSession.get();
            if (gpSession != null && gpSession.isOpen()) {
                gpSession.close();
                getLog(BaseTest.class).info("Greenplum session closed: {}", gpSession.hashCode());
            }

            Session hdSession = hadoopSession.get();
            if (hdSession != null && hdSession.isOpen()) {
                hdSession.close();
                getLog(BaseTest.class).info("Hadoop session closed: {}", hdSession.hashCode());
            }

            // Очистка всех ThreadLocal переменных
            greenplumTransaction.remove();
            greenplumSession.remove();
            baseDaoGreenplum.remove();

            hadoopTransaction.remove();
            hadoopSession.remove();
            baseDaoHadoop.remove();

            testUtil.remove();
            greenplumKerberosTicket.remove();
        }
    }

    @AfterSuite
    @Step("Закрытие sessionFactory")
    protected void tearDown() {
        if (settings.getProperty("useZephyr").equals("true") && !testResults.isEmpty()) {
            createTestRun();
            addTestResultsToTestRun(testResults);

            String issueKey = getProperty("issueKey");
            if (issueKey != null && !issueKey.isEmpty()) {
                Integer testRunId = getTestRun().getId();
                String issueId = getIssue(issueKey).getId();
                addTestRunToIssue(testRunId, issueId);
            }
        }

        if (greenplumSessionFactory != null && !greenplumSessionFactory.isClosed()) {
            greenplumSessionFactory.close();
            getLog(BaseTest.class).info("Greenplum sessionFactory closed");
        }

        if (hadoopSessionFactory != null && !hadoopSessionFactory.isClosed()) {
            SessionFactoryImplementor sfImpl = (SessionFactoryImplementor) hadoopSessionFactory;
            ServiceRegistry serviceRegistry = sfImpl.getServiceRegistry();
            ConnectionProvider connectionProvider = serviceRegistry.getService(ConnectionProvider.class);
            getLog(BaseTest.class).warn("ConnectionProvider implementation class: {}",
                    connectionProvider != null ? connectionProvider.getClass().getName() : null);
            if (connectionProvider instanceof Stoppable) {
                ((Stoppable) connectionProvider).stop("Shutting down after test suite");
                getLog(BaseTest.class).warn("Shutting down after test suite");
            }

            hadoopSessionFactory.close();
            getLog(BaseTest.class).info("Hadoop sessionFactory closed");
        }
    }

    // Геттеры для использования в тестах
    protected Session getGreenplumSession() {
        return greenplumSession.get();
    }

    protected Transaction getGreenplumTransaction() {
        return greenplumTransaction.get();
    }

    protected BaseDaoGreenplum getBaseDaoGreenplum() {
        return baseDaoGreenplum.get();
    }

    protected Session getHadoopSession() {
        return hadoopSession.get();
    }

    protected Transaction getHadoopTransaction() {
        return hadoopTransaction.get();
    }

    protected BaseDaoHadoop getBaseDaoHadoop() {
        return baseDaoHadoop.get();
    }

    protected TestUtil getTestUtil() {
        return testUtil.get();
    }
}
