/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for LocalStorageProvider and LocalStorageObject.
 */
public class LocalStorageProviderTests extends ESTestCase {

    public void testReadFullFile() throws IOException {
        // Create a temporary file
        Path tempFile = createTempFile("test", ".txt");
        String content = "Hello, World!\nThis is a test file.";
        Files.writeString(tempFile, content);

        // Create storage provider and object
        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath path = StoragePath.of("file://" + tempFile.toAbsolutePath());
        StorageObject object = provider.newObject(path);

        // Read the full file
        try (
            InputStream stream = object.newStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        ) {
            String line1 = reader.readLine();
            String line2 = reader.readLine();
            assertEquals("Hello, World!", line1);
            assertEquals("This is a test file.", line2);
        }
    }

    public void testReadRangeFromFile() throws IOException {
        // Create a temporary file with known content
        Path tempFile = createTempFile("test", ".txt");
        String content = "0123456789ABCDEFGHIJ";
        Files.writeString(tempFile, content);

        // Create storage provider and object
        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath path = StoragePath.of("file://" + tempFile.toAbsolutePath());
        StorageObject object = provider.newObject(path);

        // Read a range (bytes 5-9, which should be "56789")
        try (InputStream stream = object.newStream(5, 5)) {
            byte[] buffer = new byte[5];
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            assertEquals("56789", new String(buffer, StandardCharsets.UTF_8));
        }
    }

    public void testFileMetadata() throws IOException {
        // Create a temporary file
        Path tempFile = createTempFile("test", ".txt");
        String content = "Test content";
        Files.writeString(tempFile, content);

        // Create storage provider and object
        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath path = StoragePath.of("file://" + tempFile.toAbsolutePath());
        StorageObject object = provider.newObject(path);

        // Check metadata
        assertTrue(object.exists());
        assertEquals(content.length(), object.length());
        assertNotNull(object.lastModified());
    }

    public void testListDirectory() throws IOException {
        // Create a temporary directory with some files
        Path tempDir = createTempDir();
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("file2.csv");
        Files.writeString(file1, "content1");
        Files.writeString(file2, "content2");

        // Create storage provider
        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath dirPath = StoragePath.of("file://" + tempDir.toAbsolutePath());

        // List directory
        List<StorageEntry> entries = new ArrayList<>();
        try (StorageIterator iterator = provider.listObjects(dirPath, false)) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }

        // Filter out hidden files (like .DS_Store on macOS) and ExtraFS files for the assertion
        List<String> fileNames = entries.stream()
            .map(e -> e.path().objectName())
            .filter(name -> name.startsWith(".") == false && name.startsWith("extra") == false)
            .sorted()
            .toList();
        assertEquals(List.of("file1.txt", "file2.csv"), fileNames);
    }

    public void testFileNotFound() throws IOException {
        // Use a temp directory path that doesn't exist (within allowed paths)
        Path tempDir = createTempDir();
        Path nonExistentFile = tempDir.resolve("nonexistent_file.txt");

        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath path = StoragePath.of("file://" + nonExistentFile.toAbsolutePath());
        StorageObject object = provider.newObject(path);

        assertFalse(object.exists());
        expectThrows(IOException.class, () -> object.newStream());
    }

    public void testSupportedSchemes() {
        LocalStorageProvider provider = new LocalStorageProvider();
        List<String> schemes = provider.supportedSchemes();
        assertEquals(1, schemes.size());
        assertEquals("file", schemes.get(0));
    }

    public void testInvalidScheme() {
        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath path = StoragePath.of("http://example.com/file.txt");

        expectThrows(IllegalArgumentException.class, () -> provider.newObject(path));
    }

    // -- directory listing: non-recursive vs recursive --

    public void testListDirectoryNonRecursive() throws IOException {
        Path tempDir = createTempDir();
        Files.createFile(tempDir.resolve("a.parquet"));
        Files.createFile(tempDir.resolve("b.parquet"));
        Path sub = Files.createDirectories(tempDir.resolve("sub"));
        Files.createFile(sub.resolve("c.parquet"));

        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath prefix = StoragePath.of("file://" + tempDir.toAbsolutePath());

        List<String> names = collectObjectNames(provider.listObjects(prefix, false));
        assertEquals(List.of("a.parquet", "b.parquet"), sorted(names));
    }

    public void testListDirectoryRecursive() throws IOException {
        Path tempDir = createTempDir();
        Files.createFile(tempDir.resolve("a.parquet"));
        Path sub = Files.createDirectories(tempDir.resolve("sub"));
        Files.createFile(sub.resolve("c.parquet"));
        Path deep = Files.createDirectories(sub.resolve("deep"));
        Files.createFile(deep.resolve("d.parquet"));

        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath prefix = StoragePath.of("file://" + tempDir.toAbsolutePath());

        List<String> names = collectObjectNames(provider.listObjects(prefix, true));
        assertEquals(List.of("a.parquet", "c.parquet", "d.parquet"), sorted(names));
    }

    public void testListDirectoryRecursiveMultipleSubdirs() throws IOException {
        Path tempDir = createTempDir();
        for (String dir : List.of("dept_a", "dept_b", "dept_c")) {
            Path sub = Files.createDirectories(tempDir.resolve(dir));
            Files.createFile(sub.resolve("data.parquet"));
        }

        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath prefix = StoragePath.of("file://" + tempDir.toAbsolutePath());

        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));
        assertEquals(3, entries.size());
    }

    public void testListEmptyDirectoryReturnsNothing() throws IOException {
        Path tempDir = createTempDir();

        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath prefix = StoragePath.of("file://" + tempDir.toAbsolutePath());

        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));
        assertEquals(0, entries.size());
    }

    public void testListDirectoryRecursiveRandomTree() throws IOException {
        Path tempDir = createTempDir();
        String[] extensions = { ".parquet", ".csv", ".txt" };
        int totalFiles = 0;

        int dirCount = between(2, 5);
        for (int d = 0; d < dirCount; d++) {
            Path sub = Files.createDirectories(tempDir.resolve("dir_" + d));
            int fileCount = between(1, 4);
            for (int f = 0; f < fileCount; f++) {
                String ext = extensions[random().nextInt(extensions.length)];
                Files.createFile(sub.resolve("file_" + f + ext));
                totalFiles++;
            }
            if (randomBoolean()) {
                Path deep = Files.createDirectories(sub.resolve("nested"));
                int deepCount = between(1, 3);
                for (int f = 0; f < deepCount; f++) {
                    String ext = extensions[random().nextInt(extensions.length)];
                    Files.createFile(deep.resolve("deep_" + f + ext));
                    totalFiles++;
                }
            }
        }

        LocalStorageProvider provider = new LocalStorageProvider();
        StoragePath prefix = StoragePath.of("file://" + tempDir.toAbsolutePath());

        List<StorageEntry> entries = collectAll(provider.listObjects(prefix, true));
        assertEquals(totalFiles, entries.size());

        // Non-recursive should find zero files since all files are in subdirs
        List<StorageEntry> flatEntries = collectAll(provider.listObjects(prefix, false));
        assertEquals(0, flatEntries.size());
    }

    // -- helpers --

    private static List<String> collectObjectNames(StorageIterator iterator) throws IOException {
        List<String> names = new ArrayList<>();
        try (iterator) {
            while (iterator.hasNext()) {
                String name = iterator.next().path().objectName();
                // Filter out files created by Lucene's ExtraFS test infrastructure
                if (name.startsWith("extra") == false) {
                    names.add(name);
                }
            }
        }
        return names;
    }

    private static List<StorageEntry> collectAll(StorageIterator iterator) throws IOException {
        List<StorageEntry> entries = new ArrayList<>();
        try (iterator) {
            while (iterator.hasNext()) {
                StorageEntry entry = iterator.next();
                // Filter out files created by Lucene's ExtraFS test infrastructure
                if (entry.path().objectName().startsWith("extra") == false) {
                    entries.add(entry);
                }
            }
        }
        return entries;
    }

    private static List<String> sorted(List<String> list) {
        List<String> copy = new ArrayList<>(list);
        copy.sort(String::compareTo);
        return copy;
    }
}
