import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipExtractor {

    /**
     * Распаковывает ZIP-архив и возвращает список абсолютных путей к распакованным файлам.
     *
     * @param absoluteZipPath абсолютный путь до ZIP-архива
     * @return список абсолютных путей к распакованным файлам
     * @throws IOException если файл не найден или возникает ошибка при распаковке
     */
    public static List<String> unzip(String absoluteZipPath) throws IOException {
        File zipFile = new File(absoluteZipPath);
        if (!zipFile.exists()) {
            throw new FileNotFoundException("ZIP файл не найден: " + absoluteZipPath);
        }

        File destDir = zipFile.getParentFile();
        List<String> extractedFilePaths = new ArrayList<>();

        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                File extractedFile = new File(destDir, entry.getName());

                // Защита от Zip Slip (вредоносные архивы с путями типа ../../file)
                String destDirPath = destDir.getCanonicalPath();
                String extractedFilePath = extractedFile.getCanonicalPath();
                if (!extractedFilePath.startsWith(destDirPath + File.separator)) {
                    throw new IOException("Небезопасный путь внутри архива: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    extractedFile.mkdirs();
                } else {
                    File parent = extractedFile.getParentFile();
                    if (!parent.exists()) {
                        parent.mkdirs();
                    }

                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(extractedFile))) {
                        byte[] buffer = new byte[4096];
                        int read;
                        while ((read = zipIn.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                    }

                    extractedFilePaths.add(extractedFile.getAbsolutePath());
                    System.out.println("✅ Распакован файл: " + extractedFile.getName());
                }

                zipIn.closeEntry();
            }
        }

        System.out.println("🎉 Архив успешно распакован. Файлов: " + extractedFilePaths.size());
        return extractedFilePaths;
    }
}