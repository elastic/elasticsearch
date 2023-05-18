/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.blobstore.fs;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterSeekableByteChannel;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("*") // we do our own mocking
public class FsBlobContainerTests extends ESTestCase {

    final AtomicLong totalBytesRead = new AtomicLong(0);
    FileSystem fileSystem = null;

    @Before
    public void setupMockFileSystems() {
        FileSystemProvider fileSystemProvider = new MockFileSystemProvider(PathUtils.getDefaultFileSystem(), totalBytesRead::addAndGet);
        fileSystem = fileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem); // restored by restoreFileSystem in ESTestCase
    }

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystem);
    }

    public void testReadBlobRangeCorrectlySkipBytes() throws IOException {
        final String blobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final byte[] blobData = randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb

        final Path path = PathUtils.get(createTempDir().toString());
        Files.write(path.resolve(blobName), blobData);

        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.EMPTY,
            path
        );
        assertThat(totalBytesRead.get(), equalTo(0L));

        final long start = randomLongBetween(0L, Math.max(0L, blobData.length - 1));
        final long length = randomLongBetween(1L, blobData.length - start);

        try (InputStream stream = container.readBlob(blobName, start, length)) {
            assertThat(totalBytesRead.get(), equalTo(0L));
            assertThat(Streams.consumeFully(stream), equalTo(length));
            assertThat(totalBytesRead.get(), equalTo(length));
        }
    }

    public void testTempBlobName() {
        final String blobName = randomAlphaOfLengthBetween(1, 20);
        final String tempBlobName = FsBlobContainer.tempBlobName(blobName);
        assertThat(tempBlobName, startsWith("pending-"));
        assertThat(tempBlobName, containsString(blobName));
    }

    public void testIsTempBlobName() {
        final String tempBlobName = FsBlobContainer.tempBlobName(randomAlphaOfLengthBetween(1, 20));
        assertThat(FsBlobContainer.isTempBlobName(tempBlobName), is(true));
    }

    public void testDeleteIgnoringIfNotExistsDoesNotThrowFileNotFound() throws IOException {
        final String blobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final byte[] blobData = randomByteArrayOfLength(512);

        final Path path = PathUtils.get(createTempDir().toString());
        Files.write(path.resolve(blobName), blobData);

        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.EMPTY,
            path
        );

        container.deleteBlobsIgnoringIfNotExists(List.of(blobName).listIterator());
        // Should not throw exception
        container.deleteBlobsIgnoringIfNotExists(List.of(blobName).listIterator());

        assertFalse(container.blobExists(blobName));
    }

    private static BytesReference getBytesAsync(Consumer<ActionListener<OptionalBytesReference>> consumer) {
        final var bytes = getAsync(consumer);
        assertNotNull(bytes);
        assertTrue(bytes.isPresent());
        return bytes.bytesReference();
    }

    private static <T> T getAsync(Consumer<ActionListener<T>> consumer) {
        return PlainActionFuture.get(consumer::accept, 0, TimeUnit.SECONDS);
    }

    public void testCompareAndExchange() throws Exception {
        final Path path = PathUtils.get(createTempDir().toString());
        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.EMPTY,
            path
        );

        final String key = randomAlphaOfLength(10);
        final AtomicReference<BytesReference> expectedValue = new AtomicReference<>(BytesArray.EMPTY);

        for (int i = 0; i < 5; i++) {
            switch (between(1, 4)) {
                case 1 -> assertEquals(expectedValue.get(), getBytesAsync(l -> container.getRegister(key, l)));
                case 2 -> assertFalse(
                    getAsync(
                        l -> container.compareAndSetRegister(
                            key,
                            randomValueOtherThan(expectedValue.get(), () -> new BytesArray(randomByteArrayOfLength(8))),
                            new BytesArray(randomByteArrayOfLength(8)),
                            l
                        )
                    )
                );
                case 3 -> assertEquals(
                    expectedValue.get(),
                    getBytesAsync(
                        l -> container.compareAndExchangeRegister(
                            key,
                            randomValueOtherThan(expectedValue.get(), () -> new BytesArray(randomByteArrayOfLength(8))),
                            new BytesArray(randomByteArrayOfLength(8)),
                            l
                        )
                    )
                );
                case 4 -> {
                    /* no-op */
                }
            }

            final var newValue = new BytesArray(randomByteArrayOfLength(8));
            if (randomBoolean()) {
                assertTrue(getAsync(l -> container.compareAndSetRegister(key, expectedValue.get(), newValue, l)));
            } else {
                assertEquals(
                    expectedValue.get(),
                    getBytesAsync(l -> container.compareAndExchangeRegister(key, expectedValue.get(), newValue, l))
                );
            }
            expectedValue.set(newValue);
        }

        container.writeBlob(key, new BytesArray(new byte[9]), false);
        expectThrows(
            IllegalStateException.class,
            () -> getBytesAsync(l -> container.compareAndExchangeRegister(key, expectedValue.get(), BytesArray.EMPTY, l))
        );
    }

    public void testAtomicWriteMetadataWithoutAtomicOverwrite() throws IOException {
        this.fileSystem = new FilterFileSystemProvider("nooverwritefs://", fileSystem) {
            @Override
            public void move(Path source, Path target, CopyOption... options) throws IOException {
                if (Set.of(options).contains(StandardCopyOption.ATOMIC_MOVE) && Files.exists(target)) {
                    // simulate a file system that can't do atomic move + overwrite
                    throw new IOException("no atomic overwrite moves");
                } else {
                    super.move(source, target, options);
                }
            }
        }.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem); // restored by restoreFileSystem in ESTestCase
        checkAtomicWrite();
    }

    public void testAtomicWriteDefaultFs() throws Exception {
        restoreFileSystem();
        checkAtomicWrite();
    }

    private static void checkAtomicWrite() throws IOException {
        final String blobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final Path path = PathUtils.get(createTempDir().toString());

        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.EMPTY,
            path
        );
        container.writeBlobAtomic(blobName, new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512))), true);
        final var blobData = new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512)));
        container.writeBlobAtomic(blobName, blobData, false);
        assertEquals(blobData, Streams.readFully(container.readBlob(blobName)));
        expectThrows(
            FileAlreadyExistsException.class,
            () -> container.writeBlobAtomic(blobName, new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512))), true)
        );
        for (String blob : container.listBlobs().keySet()) {
            assertFalse("unexpected temp blob [" + blob + "]", FsBlobContainer.isTempBlobName(blob));
        }
    }

    static class MockFileSystemProvider extends FilterFileSystemProvider {

        final Consumer<Long> onRead;

        MockFileSystemProvider(FileSystem inner, Consumer<Long> onRead) {
            super("mockfs://", inner);
            this.onRead = onRead;
        }

        private int onRead(int read) {
            if (read != -1) {
                onRead.accept((long) read);
            }
            return read;
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> opts, FileAttribute<?>... attrs) throws IOException {
            return new FilterSeekableByteChannel(super.newByteChannel(path, opts, attrs)) {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return onRead(super.read(dst));
                }
            };
        }

        @Override
        public InputStream newInputStream(Path path, OpenOption... opts) throws IOException {
            // no super.newInputStream(path, opts) as it will use the delegating FileSystem to open a SeekableByteChannel
            // and instead we want the mocked newByteChannel() method to be used
            return new FilterInputStream(delegate.newInputStream(path, opts)) {
                @Override
                public int read() throws IOException {
                    return onRead(super.read());
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return onRead(super.read(b, off, len));
                }
            };
        }
    }
}
