import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;

public class CsvTransformerBuffered {

    public static void transformCsvFile(String filePath, String schemaName, String tableName, Charset encoding) throws IOException {
        Path inputPath = Paths.get(filePath);
        Path tempPath = Files.createTempFile("transformed_", ".csv");

        try (
            BufferedReader reader = Files.newBufferedReader(inputPath, encoding);
            BufferedWriter writer = Files.newBufferedWriter(tempPath, encoding)
        ) {
            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
                String transformedLine = line.replace(",", ";");

                if (isFirstLine) {
                    transformedLine = "tablename;" + transformedLine;
                    isFirstLine = false;
                } else {
                    transformedLine = schemaName + "." + tableName + ";" + transformedLine;
                }

                writer.write(transformedLine);
                writer.newLine();
            }
        }

        Files.move(tempPath, inputPath, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void renameCsvFile(String filePath, String schemaName, String tableName) throws IOException {
        Path originalPath = Paths.get(filePath);
        Path parentDir = originalPath.getParent();
        String newFileName = schemaName + "." + tableName + "_src.csv";
        Path newPath = parentDir.resolve(newFileName);

        Files.move(originalPath, newPath, StandardCopyOption.REPLACE_EXISTING);
    }
}