/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.blobstore.fs;

import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterSeekableByteChannel;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomNonDataPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
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

        try (InputStream stream = container.readBlob(randomPurpose(), blobName, start, length)) {
            assertThat(totalBytesRead.get(), equalTo(0L));
            assertThat(Streams.consumeFully(stream), equalTo(length));
            assertThat(totalBytesRead.get(), equalTo(length));
        }
    }

    public void testReadAfterBlobLengthThrowsRequestedRangeNotSatisfiedException() throws IOException {
        final var blobName = "blob";
        final byte[] blobData = randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb

        final Path path = PathUtils.get(createTempDir().toString());
        Files.write(path.resolve(blobName), blobData);

        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, true),
            BlobPath.EMPTY,
            path
        );

        {
            long position = randomLongBetween(blobData.length, Long.MAX_VALUE - 1L);
            long length = randomLongBetween(1L, Long.MAX_VALUE - position);
            var exception = expectThrows(
                RequestedRangeNotSatisfiedException.class,
                () -> container.readBlob(randomPurpose(), blobName, position, length)
            );
            assertThat(
                exception.getMessage(),
                equalTo("Requested range [position=" + position + ", length=" + length + "] cannot be satisfied for [" + blobName + ']')
            );
        }

        {
            long position = randomLongBetween(0L, Math.max(0L, blobData.length - 1));
            long maxLength = blobData.length - position;
            long length = randomLongBetween(maxLength + 1L, Long.MAX_VALUE - 1L);
            try (var stream = container.readBlob(randomPurpose(), blobName, position, length)) {
                assertThat(totalBytesRead.get(), equalTo(0L));
                assertThat(Streams.consumeFully(stream), equalTo(maxLength));
                assertThat(totalBytesRead.get(), equalTo(maxLength));
            }
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

        container.deleteBlobsIgnoringIfNotExists(randomPurpose(), List.of(blobName).listIterator());
        // Should not throw exception
        container.deleteBlobsIgnoringIfNotExists(randomPurpose(), List.of(blobName).listIterator());

        assertFalse(container.blobExists(randomPurpose(), blobName));
    }

    private static BytesReference getBytesAsync(Consumer<ActionListener<OptionalBytesReference>> consumer) {
        final var bytes = getAsync(consumer);
        assertNotNull(bytes);
        assertTrue(bytes.isPresent());
        return bytes.bytesReference();
    }

    private static <T> T getAsync(Consumer<ActionListener<T>> consumer) {
        final var listener = SubscribableListener.newForked(consumer::accept);
        assertTrue(listener.isDone());
        return safeAwait(listener);
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
                case 1 -> assertEquals(expectedValue.get(), getBytesAsync(l -> container.getRegister(randomPurpose(), key, l)));
                case 2 -> assertFalse(
                    getAsync(
                        l -> container.compareAndSetRegister(
                            randomPurpose(),
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
                            randomPurpose(),
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
                assertTrue(getAsync(l -> container.compareAndSetRegister(randomPurpose(), key, expectedValue.get(), newValue, l)));
            } else {
                assertEquals(
                    expectedValue.get(),
                    getBytesAsync(l -> container.compareAndExchangeRegister(randomPurpose(), key, expectedValue.get(), newValue, l))
                );
            }
            expectedValue.set(newValue);
        }

        container.writeBlob(randomPurpose(), key, new BytesArray(new byte[33]), false);
        assertThat(
            safeAwaitFailure(
                OptionalBytesReference.class,
                l -> container.compareAndExchangeRegister(randomPurpose(), key, expectedValue.get(), BytesArray.EMPTY, l)
            ),
            instanceOf(IllegalStateException.class)
        );
    }

    public void testRegisterContention() throws Exception {
        final Path path = PathUtils.get(createTempDir().toString());
        final FsBlobContainer container = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false),
            BlobPath.EMPTY,
            path
        );

        final String contendedKey = randomAlphaOfLength(10);
        final String uncontendedKey = randomAlphaOfLength(10);

        final var startValue = new BytesArray(randomByteArrayOfLength(8));
        final var finalValue = randomValueOtherThan(startValue, () -> new BytesArray(randomByteArrayOfLength(8)));

        final var p = randomPurpose();
        assertTrue(safeAwait(l -> container.compareAndSetRegister(p, contendedKey, BytesArray.EMPTY, startValue, l)));
        assertTrue(safeAwait(l -> container.compareAndSetRegister(p, uncontendedKey, BytesArray.EMPTY, startValue, l)));

        final var threads = new Thread[between(2, 5)];
        final var startBarrier = new CyclicBarrier(threads.length + 1);
        final var casSucceeded = new AtomicBoolean();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(
                i == 0
                    // first thread does an uncontended write, which must succeed
                    ? () -> {
                        safeAwait(startBarrier);
                        final OptionalBytesReference result = safeAwait(
                            l -> container.compareAndExchangeRegister(p, uncontendedKey, startValue, finalValue, l)
                        );
                        // NB calling .bytesReference() asserts that the result is present, there was no contention
                        assertEquals(startValue, result.bytesReference());
                    }
                    // other threads try and do contended writes, which may fail and need retrying
                    : () -> {
                        safeAwait(startBarrier);
                        while (casSucceeded.get() == false) {
                            final OptionalBytesReference result = safeAwait(
                                l -> container.compareAndExchangeRegister(p, contendedKey, startValue, finalValue, l)
                            );
                            if (result.isPresent() && result.bytesReference().equals(startValue)) {
                                casSucceeded.set(true);
                            }
                        }
                    },
                "write-thread-" + i
            );
            threads[i].start();
        }

        safeAwait(startBarrier);
        while (casSucceeded.get() == false) {
            for (var key : new String[] { contendedKey, uncontendedKey }) {
                // NB calling .bytesReference() asserts that the read did not experience contention
                assertThat(
                    safeAwait((ActionListener<OptionalBytesReference> l) -> container.getRegister(p, key, l)).bytesReference(),
                    oneOf(startValue, finalValue)
                );
            }
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (var key : new String[] { contendedKey, uncontendedKey }) {
            assertEquals(
                finalValue,
                safeAwait((ActionListener<OptionalBytesReference> l) -> container.getRegister(p, key, l)).bytesReference()
            );
        }
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
        final var randomData = new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512)));
        if (randomBoolean()) {
            container.writeBlobAtomic(randomNonDataPurpose(), blobName, randomData, true);
        } else {
            container.writeBlobAtomic(randomNonDataPurpose(), blobName, randomData.streamInput(), randomData.length(), true);
        }
        final var blobData = new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512)));
        if (randomBoolean()) {
            container.writeBlobAtomic(randomNonDataPurpose(), blobName, blobData, false);
        } else {
            container.writeBlobAtomic(randomNonDataPurpose(), blobName, blobData.streamInput(), blobData.length(), false);
        }
        assertEquals(blobData, Streams.readFully(container.readBlob(randomPurpose(), blobName)));
        expectThrows(
            FileAlreadyExistsException.class,
            () -> container.writeBlobAtomic(
                randomNonDataPurpose(),
                blobName,
                new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512))),
                true
            )
        );
        for (String blob : container.listBlobs(randomPurpose()).keySet()) {
            assertFalse("unexpected temp blob [" + blob + "]", FsBlobContainer.isTempBlobName(blob));
        }
    }

    public void testCopy() throws Exception {
        // without this, on CI the test sometimes fails with
        // java.nio.file.ProviderMismatchException: mismatch, expected: class org.elasticsearch.common.blobstore.fs.FsBlobContainerTests$1,
        // got: class org.elasticsearch.common.blobstore.fs.FsBlobContainerTests$MockFileSystemProvider
        // and I haven't figured out why yet.
        restoreFileSystem();
        final var path = PathUtils.get(createTempDir().toString());
        final var store = new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false);
        final var sourcePath = BlobPath.EMPTY.add("source");
        final var sourceContainer = store.blobContainer(sourcePath);
        final var destinationPath = BlobPath.EMPTY.add("destination");
        final var destinationContainer = store.blobContainer(destinationPath);

        final var sourceBlobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final var blobName = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        final var contents = new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 512)));
        sourceContainer.writeBlob(randomPurpose(), sourceBlobName, contents, true);
        destinationContainer.copyBlob(randomPurpose(), sourceContainer, sourceBlobName, blobName, contents.length());

        var sourceContents = Streams.readFully(sourceContainer.readBlob(randomPurpose(), sourceBlobName));
        var targetContents = Streams.readFully(destinationContainer.readBlob(randomPurpose(), blobName));
        assertEquals(sourceContents, targetContents);
        assertEquals(contents, targetContents);
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
