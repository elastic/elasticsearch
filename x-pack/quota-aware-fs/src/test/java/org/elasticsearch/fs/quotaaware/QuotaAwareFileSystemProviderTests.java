/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.fs.quotaaware;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts.Limit;
import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.security.PrivilegedActionException;
import java.util.Properties;
import java.util.Random;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.elasticsearch.fs.quotaaware.QuotaAwareFileSystemProvider.QUOTA_PATH_KEY;
import static org.hamcrest.Matchers.startsWith;

@Limit(bytes = 10000)
@SuppressForbidden(reason = "accesses the default filesystem by design")
public class QuotaAwareFileSystemProviderTests extends LuceneTestCase {

    public void testSystemPropertyShouldBeSet() {
        FileSystemProvider systemProvider = FileSystems.getDefault().provider();
        System.clearProperty(QUOTA_PATH_KEY);

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new QuotaAwareFileSystemProvider(systemProvider)
        );

        assertThat(exception.getMessage(), startsWith("Property " + QUOTA_PATH_KEY + " must be set to a URI"));
    }

    public void testInitiallyNoQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = FileSystems.getDefault().provider();
        Throwable cause = null;

        try (QuotaAwareFileSystemProvider ignored = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            fail(); //
        } catch (PrivilegedActionException e) {
            cause = e.getCause();
        }
        assertTrue("Should be FileNotFoundException", cause instanceof NoSuchFileException);
    }

    public void testBasicQuotaFile() throws Exception {
        doValidFileTest(500, 200);
    }

    public void testUpdateQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(500, provider.getTotal());
            assertEquals(200, provider.getRemaining());
            writeQuota(450, 150, systemProvider, quotaFile);
            withRetry(2000, 500, () -> {
                assertEquals(450, provider.getTotal());
                assertEquals(150, provider.getRemaining());
            });
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/70680")
    public void testRepeatedUpdate() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);
        Random random = new Random();
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            for (int i = 0; i < 10; i++) {
                long expectedTotal = Math.abs(random.nextLong());
                long expectedRemaining = Math.abs(random.nextLong());
                writeQuota(expectedTotal, expectedRemaining, systemProvider, quotaFile);
                withRetry(2000, 100, () -> {
                    assertEquals(expectedTotal, provider.getTotal());
                    assertEquals(expectedRemaining, provider.getRemaining());
                });
            }
        }
    }

    public void testEventuallyMissingQuotaFile() throws Exception {

        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(500, provider.getTotal());
            assertEquals(200, provider.getRemaining());

            systemProvider.delete(quotaFile);

            withRetry(2000, 500, () -> {
                boolean gotError = false;
                try {
                    provider.getTotal();
                } catch (AssertionError e) {
                    gotError = true;
                }
                assertTrue(gotError);
            });
        }
    }

    public void testEventuallyMalformedQuotaFile() throws Exception {

        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(500, provider.getTotal());
            assertEquals(200, provider.getRemaining());

            try (
                OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, TRUNCATE_EXISTING);
                OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
                PrintWriter printWriter = new PrintWriter(streamWriter)
            ) {
                printWriter.write("This is not valid properties file syntax");
            }

            withRetry(2000, 500, () -> {
                boolean gotError = false;
                try {
                    provider.getTotal();
                } catch (AssertionError e) {
                    gotError = true;
                }
                assertTrue(gotError);
            });
        }
    }

    public void testHighQuotaFile() throws Exception {
        doValidFileTest(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 2L);
    }

    public void testMalformedNumberInQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        quota.setProperty("total", "ThisNotANumber");
        quota.setProperty("remaining", "1");
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#malformedNumberInQuotaFile");
        }

        expectThrows(NumberFormatException.class, () -> new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri()));
    }

    public void testMalformedQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();

        try (
            OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW);
            OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
            PrintWriter printWriter = new PrintWriter(streamWriter)
        ) {
            printWriter.write("This is not valid properties file syntax");
        }

        expectThrows(Exception.class, () -> new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri()));
    }

    public void testFileStoreLimited() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        long expectedTotal = 500;
        long expectedRemaining = 200;
        quota.setProperty("total", Long.toString(expectedTotal));
        quota.setProperty("remaining", Long.toString(expectedRemaining));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#fileStoreLimited");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            Path path = createTempFile();
            FileStore fileStore = provider.getFileStore(path);
            assertEquals(expectedTotal, fileStore.getTotalSpace());
            assertEquals(expectedRemaining, fileStore.getUsableSpace());
            assertEquals(expectedRemaining, fileStore.getUnallocatedSpace());
        }
    }

    public void testFileStoreNotLimited() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        DelegatingProvider snapshotProvider = new SnapshotFilesystemProvider(quotaFile.getFileSystem().provider());

        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(Long.MAX_VALUE));
        quota.setProperty("remaining", Long.toString(Long.MAX_VALUE));

        try (OutputStream stream = snapshotProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#fileStoreNotLimited");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(snapshotProvider, quotaFile.toUri())) {
            Path path = createTempFile();
            FileStore fileStore = provider.getFileStore(path);
            FileStore unLimitedStore = snapshotProvider.getFileStore(path);
            assertEquals(unLimitedStore.getTotalSpace(), fileStore.getTotalSpace());
            assertEquals(unLimitedStore.getUsableSpace(), fileStore.getUsableSpace());
            assertEquals(unLimitedStore.getUnallocatedSpace(), fileStore.getUnallocatedSpace());
        }
    }

    public void testDefaultFilesystemIsPreinitialized() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(Long.MAX_VALUE));
        quota.setProperty("remaining", Long.toString(Long.MAX_VALUE));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#defaultFilesystemIsPreinitialized");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertNotNull(provider.getFileSystem(new URI("file:///")));
        }
    }

    /**
     * Mimics a cyclic reference that may happen when
     * {@link QuotaAwareFileSystemProvider} is installed as the default provider
     * in the JVM and the delegate provider references
     * {@link FileSystems#getDefault()} by for instance relying on
     * {@link File#toPath()}
     */
    public void testHandleReflexiveDelegate() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        DelegatingProvider cyclicProvider = new DelegatingProvider(systemProvider) {
            @Override
            @SuppressForbidden(reason = "Uses new File() to work around test issue on Windows")
            public Path getPath(URI uri) {
                try {
                    // This convoluted line is necessary in order to get a valid path on Windows.
                    final String uriPath = new File(uri.getPath()).toPath().toString();
                    return cyclicReference.getFileSystem(new URI("file:///")).getPath(uriPath);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(Long.MAX_VALUE));
        quota.setProperty("remaining", Long.toString(Long.MAX_VALUE));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#testHandleReflexiveDelegate");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(cyclicProvider, quotaFile.toUri())) {
            cyclicProvider.cyclicReference = provider;
            assertNotNull(provider.getPath(new URI("file:///")));
        }
    }

    /**
     * If the implementation of {@link QuotaAwareFileSystemProvider#createLink(Path, Path)}
     * doesn't unwrap its {@link Path} arguments, it causes a runtime exception, so exercise
     * this method to check that it unwraps correctly.
     */
    public void testCreateLinkUnwrapsPaths() throws Exception {
        final Path tempDir = createTempDir();
        Path quotaFile = tempDir.resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500, 200, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            final Path quotaFilePath = provider.getPath(tempDir.resolve("path1.txt").toUri());
            final Path quotaLinkPath = provider.getPath(tempDir.resolve("path2.txt").toUri());

            Files.writeString(quotaFilePath, "some text");

            provider.createLink(quotaLinkPath, quotaFilePath);
        }
    }

    /**
     * If the implementation of {@link QuotaAwareFileSystemProvider#createSymbolicLink(Path, Path, FileAttribute[])}
     * doesn't unwrap its {@link Path} arguments, it causes a runtime exception, so exercise
     * this method to check that it unwraps correctly.
     */
    public void testCreateSymbolicLinkUnwrapsPaths() throws Exception {
        final Path tempDir = createTempDir();
        Path quotaFile = tempDir.resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500, 200, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            final Path quotaFilePath = provider.getPath(tempDir.resolve("path1.txt").toUri());
            final Path quotaLinkPath = provider.getPath(tempDir.resolve("path2.txt").toUri());

            Files.writeString(quotaFilePath, "some text");

            provider.createSymbolicLink(quotaLinkPath, quotaFilePath);
        }
    }

    private void doValidFileTest(long expectedTotal, long expectedRemaining) throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(expectedTotal, expectedRemaining, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(expectedTotal, provider.getTotal());
            assertEquals(expectedRemaining, provider.getRemaining());
        }
    }

    private void writeQuota(long expectedTotal, long expectedRemaining, FileSystemProvider systemProvider, Path quotaFile)
        throws IOException {
        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(expectedTotal));
        quota.setProperty("remaining", Long.toString(expectedRemaining));
        try (OutputStream outputStream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE, TRUNCATE_EXISTING)) {
            // Ideally this would use atomic write and the scala allocator does,
            // but the parsing logic should be able to deal with it in either
            // case.
            quota.store(outputStream, "QuotaFile for: QuotaAwareFileSystemProviderTest#doValidFileTest");
        }
    }

    public static void withRetry(int maxMillis, int interval, Runnable func) throws Exception {
        long endBy = System.currentTimeMillis() + maxMillis;

        while (true) {
            try {
                func.run();
                break;
            } catch (AssertionError | Exception e) {
                if (System.currentTimeMillis() + interval < endBy) {
                    Thread.sleep(interval);
                    continue;
                }
                throw new IllegalStateException("Retry timed out after [" + maxMillis + "]ms", e);
            }
        }

    }

}
