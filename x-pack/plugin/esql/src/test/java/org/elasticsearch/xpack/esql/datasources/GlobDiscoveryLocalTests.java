/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Bridge test: wires GlobExpander to a real filesystem StorageProvider against
 * temp directories with empty files. Validates the full discovery pipeline
 * (path parsing -> prefix extraction -> listing -> glob filtering).
 */
@SuppressWarnings("RegexpMultiline")
public class GlobDiscoveryLocalTests extends ESTestCase {

    private Path fixtureRoot;
    private StorageProvider provider;

    @Before
    public void createFixtureTree() throws IOException {
        fixtureRoot = createTempDir();
        // Flat files
        touch("report_2024_01.parquet");
        touch("report_2024_02.parquet");
        touch("report_2024_03.csv");
        touch("summary.txt");
        // Nested structure
        touch("year/2023/q1.parquet");
        touch("year/2023/q2.parquet");
        touch("year/2024/q1.parquet");
        touch("year/2024/q2.parquet");
        touch("year/2024/q3.csv");

        provider = new TestLocalStorageProvider();
    }

    private void touch(String relativePath) throws IOException {
        Path file = fixtureRoot.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.createFile(file);
    }

    private String rootUri() {
        return "file://" + fixtureRoot.toAbsolutePath();
    }

    // -- predefined tests --

    public void testFlatStarGlob() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/*.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(2, result.size());
    }

    public void testFlatStarGlobAllExtensions() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/report_2024_*.*", provider);
        assertTrue(result.isResolved());
        assertEquals(3, result.size());
    }

    public void testFlatQuestionMarkGlob() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/report_2024_0?.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(2, result.size());
    }

    public void testFlatBraceAlternatives() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/*.{parquet,csv}", provider);
        assertTrue(result.isResolved());
        assertEquals(3, result.size());
    }

    public void testRecursiveDoubleStarGlob() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/year/**" + "/*.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(4, result.size());
    }

    public void testRecursiveDoubleStarAllFiles() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/year/**" + "/*", provider);
        assertTrue(result.isResolved());
        assertEquals(5, result.size());
    }

    public void testRecursiveSingleDirGlob() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/year/2024/*.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(2, result.size());
    }

    public void testNoMatchReturnsEmpty() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/*.json", provider);
        assertTrue(result.isEmpty());
    }

    public void testLiteralPathReturnsUnresolved() throws IOException {
        FileSet result = GlobExpander.expandGlob(rootUri() + "/summary.txt", provider);
        assertTrue(result.isUnresolved());
    }

    public void testCommaSeparatedMixed() throws IOException {
        String paths = rootUri() + "/*.parquet, " + rootUri() + "/summary.txt";
        FileSet result = GlobExpander.expandCommaSeparated(paths, provider);
        assertTrue(result.isResolved());
        // 2 parquet files from glob + 1 literal
        assertEquals(3, result.size());
    }

    // -- randomized tests --

    public void testRandomTreeStarGlob() throws IOException {
        Path root = createTempDir();
        List<String> allPaths = generateRandomTree(root, random());

        long expectedCount = allPaths.stream().filter(p -> p.contains("/") == false).filter(p -> p.endsWith(".parquet")).count();

        FileSet result = GlobExpander.expandGlob("file://" + root.toAbsolutePath() + "/*.parquet", new TestLocalStorageProvider());
        if (expectedCount == 0) {
            assertTrue("Expected EMPTY for no root .parquet files", result.isEmpty());
        } else {
            assertEquals(expectedCount, result.size());
        }
    }

    public void testRandomTreeRecursiveGlob() throws IOException {
        Path root = createTempDir();
        List<String> allPaths = generateRandomTree(root, random());

        long expectedCount = allPaths.stream().filter(p -> p.endsWith(".parquet")).count();

        FileSet result = GlobExpander.expandGlob("file://" + root.toAbsolutePath() + "/**" + "/*.parquet", new TestLocalStorageProvider());
        if (expectedCount == 0) {
            assertTrue(result.isEmpty());
        } else {
            assertEquals(expectedCount, result.size());
        }
    }

    public void testRandomTreeBraceAlternatives() throws IOException {
        Path root = createTempDir();
        List<String> allPaths = generateRandomTree(root, random());

        long expectedCount = allPaths.stream().filter(p -> p.endsWith(".parquet") || p.endsWith(".csv")).count();

        String uri = "file://" + root.toAbsolutePath() + "/**" + "/*.{parquet,csv}";
        FileSet result = GlobExpander.expandGlob(uri, new TestLocalStorageProvider());
        if (expectedCount == 0) {
            assertTrue(result.isEmpty());
        } else {
            assertEquals(expectedCount, result.size());
        }
    }

    public void testRandomTreeNonRecursiveIgnoresSubdirs() throws IOException {
        Path root = createTempDir();
        List<String> allPaths = generateRandomTree(root, random());

        long rootParquetCount = allPaths.stream().filter(p -> p.contains("/") == false).filter(p -> p.endsWith(".parquet")).count();

        long totalParquetCount = allPaths.stream().filter(p -> p.endsWith(".parquet")).count();

        TestLocalStorageProvider testProvider = new TestLocalStorageProvider();
        FileSet flatResult = GlobExpander.expandGlob("file://" + root.toAbsolutePath() + "/*.parquet", testProvider);
        FileSet recursiveResult = GlobExpander.expandGlob("file://" + root.toAbsolutePath() + "/**" + "/*.parquet", testProvider);

        long flatSize = flatResult.isEmpty() ? 0 : flatResult.size();
        long recursiveSize = recursiveResult.isEmpty() ? 0 : recursiveResult.size();

        assertEquals(rootParquetCount, flatSize);
        assertEquals(totalParquetCount, recursiveSize);
        assertTrue("Recursive should find >= non-recursive", recursiveSize >= flatSize);
    }

    public void testRandomTreeQuestionMarkGlob() throws IOException {
        Path root = createTempDir();
        int fileCount = between(1, 9);
        int expectedCount = 0;
        for (int i = 0; i < fileCount; i++) {
            String name = "f" + i + ".parquet";
            Files.createFile(root.resolve(name));
            expectedCount++;
        }
        // Two-digit name won't match f?.parquet
        Files.createFile(root.resolve("f10.parquet"));

        FileSet result = GlobExpander.expandGlob("file://" + root.toAbsolutePath() + "/f?.parquet", new TestLocalStorageProvider());
        assertEquals(expectedCount, result.size());
    }

    // -- random tree generation --

    private static List<String> generateRandomTree(Path root, Random random) throws IOException {
        String[] extensions = { ".parquet", ".csv", ".json", ".txt", ".orc" };
        String[] dirNames = { "data", "archive", "year", "dept", "region", "backup", "staging" };
        int depth = between(random, 1, 4);
        int dirsPerLevel = between(random, 1, 4);
        int filesPerDir = between(random, 0, 6);

        List<String> allPaths = new ArrayList<>();
        generateLevel(root, "", dirNames, extensions, random, depth, dirsPerLevel, filesPerDir, allPaths);
        return allPaths;
    }

    private static void generateLevel(
        Path current,
        String relativeSoFar,
        String[] dirNames,
        String[] extensions,
        Random random,
        int remainingDepth,
        int dirsPerLevel,
        int filesPerDir,
        List<String> allPaths
    ) throws IOException {
        int fileCount = between(random, 0, filesPerDir);
        for (int i = 0; i < fileCount; i++) {
            String ext = extensions[random.nextInt(extensions.length)];
            String fileName = "file_" + i + ext;
            String relativePath = relativeSoFar.isEmpty() ? fileName : relativeSoFar + "/" + fileName;
            Files.createFile(current.resolve(fileName));
            allPaths.add(relativePath);
        }

        if (remainingDepth > 0) {
            int dirCount = between(random, 1, dirsPerLevel);
            for (int d = 0; d < dirCount; d++) {
                String dirName = dirNames[random.nextInt(dirNames.length)] + "_" + d;
                Path subDir = Files.createDirectories(current.resolve(dirName));
                String newRelative = relativeSoFar.isEmpty() ? dirName : relativeSoFar + "/" + dirName;
                generateLevel(subDir, newRelative, dirNames, extensions, random, remainingDepth - 1, dirsPerLevel, filesPerDir, allPaths);
            }
        }
    }

    private static int between(Random random, int min, int max) {
        return min + random.nextInt(max - min + 1);
    }

    // -- inline filesystem StorageProvider for testing --

    private static class TestLocalStorageProvider implements StorageProvider {

        @Override
        public StorageObject newObject(StoragePath path) {
            return new TestStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new TestStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new TestStorageObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
            Path dirPath = PathUtils.get(prefix.path());
            if (Files.exists(dirPath) == false || Files.isDirectory(dirPath) == false) {
                return emptyIterator();
            }

            List<StorageEntry> entries = new ArrayList<>();
            if (recursive) {
                Files.walkFileTree(dirPath, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (attrs.isRegularFile()) {
                            entries.add(toEntry(file, attrs));
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath)) {
                    for (Path entry : stream) {
                        BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class);
                        if (attrs.isRegularFile()) {
                            entries.add(toEntry(entry, attrs));
                        }
                    }
                }
            }

            Iterator<StorageEntry> it = entries.iterator();
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StorageEntry next() {
                    if (it.hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return it.next();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean exists(StoragePath path) {
            return Files.exists(PathUtils.get(path.path()));
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("file");
        }

        @Override
        public void close() {}

        private static StorageEntry toEntry(Path file, BasicFileAttributes attrs) {
            StoragePath storagePath = StoragePath.of("file://" + file.toAbsolutePath());
            return new StorageEntry(storagePath, attrs.size(), attrs.lastModifiedTime().toInstant());
        }

        private static StorageIterator emptyIterator() {
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public StorageEntry next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }
    }

    private static class TestStorageObject implements StorageObject {
        private final StoragePath path;

        TestStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return Files.exists(PathUtils.get(path.path()));
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
