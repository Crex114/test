@Listeners(TestResultListener.class)
public class BaseTest {
    protected static SessionFactory greenplumSessionFactory;
    protected static SessionFactory hadoopSessionFactory;

    private static final ThreadLocal<Subject> greenplumKerberosTicket = new ThreadLocal<>();
    protected static final ThreadLocal<Session> greenplumSession = new ThreadLocal<>();
    protected static final ThreadLocal<Transaction> greenplumTransaction = new ThreadLocal<>();
    protected static final ThreadLocal<BaseDaoGreenplum> baseDaoGreenplum = new ThreadLocal<>();
    protected static final ThreadLocal<TestUtil> testUtil = new ThreadLocal<>();

    protected Session hadoopSession;
    protected Transaction hadoopTransaction;
    protected BaseDaoHadoop baseDaoHadoop;
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
                    Session session = greenplumSessionFactory.openSession();
                    greenplumSession.set(session);
                    getLog(BaseTest.class).info("Greenplum session opened: {}", session.hashCode());

                    if (session == null || !session.isOpen()) {
                        throw new RuntimeException("Greenplum session is null or not open");
                    }

                    Transaction transaction = session.beginTransaction();
                    greenplumTransaction.set(transaction);

                    getLog(BaseTest.class).info("Greenplum transaction {} started for session {}",
                            transaction.hashCode(), session.hashCode());

                    BaseDaoGreenplum dao = new BaseDaoGreenplumImpl(session, transaction);
                    baseDaoGreenplum.set(dao);
                }

                if (settings.getProperty("connectToHadoop").equals("true")) {
                    hadoopSession = hadoopSessionFactory.openSession();
                    hadoopTransaction = hadoopSession.beginTransaction();
                    baseDaoHadoop = new BaseDaoHadoopImpl(hadoopSession);
                    getLog(BaseTest.class).info("Hadoop transaction started");
                }

                softly = new SoftAssert();

                testUtil.set(new TestUtil(greenplumSession.get(), greenplumTransaction.get(), softly,
                        baseDaoGreenplum.get(), baseDaoHadoop));

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
            Transaction transaction = greenplumTransaction.get();
            Session session = greenplumSession.get();

            if (transaction != null && transaction.isActive()) {
                transaction.commit();
                getLog(BaseTest.class).info("Greenplum transaction commit {}", transaction.hashCode());
            }
        } catch (Exception e) {
            Transaction transaction = greenplumTransaction.get();
            if (transaction != null && transaction.isActive()) {
                transaction.rollback();
            }
            getLog(BaseTest.class).error("Error in Greenplum transaction commit", e);
        } finally {
            Session gpSession = greenplumSession.get();
            if (gpSession != null && gpSession.isOpen()) {
                gpSession.close();
                getLog(BaseTest.class).info("Greenplum session closed: {}", gpSession.hashCode());
            }

            greenplumTransaction.remove();
            greenplumSession.remove();
            baseDaoGreenplum.remove();
            testUtil.remove();
            greenplumKerberosTicket.remove();

            if (hadoopSession != null && hadoopSession.isOpen()) {
                hadoopSession.close();
                getLog(BaseTest.class).info("Hadoop session closed");
            }
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

    // Геттеры для использования в тестах, если требуется
    protected Session getGreenplumSession() {
        return greenplumSession.get();
    }

    protected Transaction getGreenplumTransaction() {
        return greenplumTransaction.get();
    }

    protected BaseDaoGreenplum getBaseDaoGreenplum() {
        return baseDaoGreenplum.get();
    }

    protected TestUtil getTestUtil() {
        return testUtil.get();
    }
}
