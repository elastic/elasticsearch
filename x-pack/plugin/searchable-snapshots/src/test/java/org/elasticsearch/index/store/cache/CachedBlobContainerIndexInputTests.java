/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.SearchableSnapshotRecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.index.store.cache.TestUtils.createCacheService;
import static org.elasticsearch.index.store.cache.TestUtils.singleBlobContainer;
import static org.elasticsearch.index.store.cache.TestUtils.singleSplitBlobContainer;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CachedBlobContainerIndexInputTests extends ESIndexInputTestCase {

    public void testRandomReads() throws IOException {
        final ThreadPool threadPool = new TestThreadPool(getTestName(), SearchableSnapshots.executorBuilders());
        try (CacheService cacheService = createCacheService(random())) {
            cacheService.start();

            SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            IndexId indexId = new IndexId("_name", "_uuid");
            ShardId shardId = new ShardId("_name", "_uuid", 0);

            for (int i = 0; i < 5; i++) {
                final String fileName = randomAlphaOfLength(10);
                final byte[] input = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);

                final String blobName = randomUnicodeOfLength(10);
                final StoreFileMetadata metadata = new StoreFileMetadata(fileName, input.length, "_na", Version.CURRENT.luceneVersion);

                final int partSize = randomBoolean() ? input.length : randomIntBetween(1, input.length);

                final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(
                    snapshotId.getName(),
                    0L,
                    List.of(new BlobStoreIndexShardSnapshot.FileInfo(blobName, metadata, new ByteSizeValue(partSize))),
                    0L,
                    0L,
                    0,
                    0L
                );

                final boolean prewarmEnabled = randomBoolean();
                final BlobContainer singleBlobContainer = singleSplitBlobContainer(blobName, input, partSize);
                final BlobContainer blobContainer;
                if (input.length == partSize && input.length <= cacheService.getCacheSize() && prewarmEnabled == false) {
                    blobContainer = new CountingBlobContainer(singleBlobContainer, cacheService.getRangeSize());
                } else {
                    blobContainer = singleBlobContainer;
                }

                final Path shardDir;
                try {
                    shardDir = new NodeEnvironment.NodePath(createTempDir()).resolve(shardId);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
                final Path cacheDir = createTempDir();
                try (
                    SearchableSnapshotDirectory directory = new SearchableSnapshotDirectory(
                        () -> blobContainer,
                        () -> snapshot,
                        snapshotId,
                        indexId,
                        shardId,
                        Settings.builder()
                            .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                            .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), prewarmEnabled)
                            .build(),
                        () -> 0L,
                        cacheService,
                        cacheDir,
                        shardPath,
                        threadPool
                    )
                ) {
                    ShardRouting shardRouting = TestShardRouting.newShardRouting(
                        randomAlphaOfLength(10),
                        0,
                        randomAlphaOfLength(10),
                        true,
                        ShardRoutingState.INITIALIZING
                    );
                    RecoveryState recoveryState = createRecoveryState();
                    final boolean loaded = directory.loadSnapshot(recoveryState);
                    assertThat("Failed to load snapshot", loaded, is(true));
                    assertThat("Snapshot should be loaded", directory.snapshot(), notNullValue());
                    assertThat("BlobContainer should be loaded", directory.blobContainer(), notNullValue());

                    try (IndexInput indexInput = directory.openInput(fileName, newIOContext(random()))) {
                        assertEquals(input.length, indexInput.length());
                        assertEquals(0, indexInput.getFilePointer());
                        byte[] output = randomReadAndSlice(indexInput, input.length);
                        assertArrayEquals(input, output);
                    }
                }

                if (blobContainer instanceof CountingBlobContainer) {
                    long numberOfRanges = TestUtils.numberOfRanges(input.length, cacheService.getRangeSize());
                    assertThat(
                        "Expected " + numberOfRanges + " ranges fetched from the source",
                        ((CountingBlobContainer) blobContainer).totalOpens.sum(),
                        equalTo(numberOfRanges)
                    );
                    assertThat(
                        "All bytes should have been read from source",
                        ((CountingBlobContainer) blobContainer).totalBytes.sum(),
                        equalTo((long) input.length)
                    );
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testThrowsEOFException() throws IOException {
        try (CacheService cacheService = createCacheService(random())) {
            cacheService.start();

            SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            IndexId indexId = new IndexId("_name", "_uuid");
            ShardId shardId = new ShardId("_name", "_uuid", 0);

            final String fileName = randomAlphaOfLength(10);
            final byte[] input = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);

            final String blobName = randomUnicodeOfLength(10);
            final StoreFileMetadata metadata = new StoreFileMetadata(fileName, input.length + 1, "_na", Version.CURRENT.luceneVersion);
            final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(
                snapshotId.getName(),
                0L,
                List.of(new BlobStoreIndexShardSnapshot.FileInfo(blobName, metadata, new ByteSizeValue(input.length + 1))),
                0L,
                0L,
                0,
                0L
            );

            final BlobContainer blobContainer = singleBlobContainer(blobName, input);
            final ThreadPool threadPool = new TestThreadPool(getTestName(), SearchableSnapshots.executorBuilders());
            final Path shardDir;
            try {
                shardDir = new NodeEnvironment.NodePath(createTempDir()).resolve(shardId);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
            final Path cacheDir = createTempDir();
            try (
                SearchableSnapshotDirectory searchableSnapshotDirectory = new SearchableSnapshotDirectory(
                    () -> blobContainer,
                    () -> snapshot,
                    snapshotId,
                    indexId,
                    shardId,
                    Settings.EMPTY,
                    () -> 0L,
                    cacheService,
                    cacheDir,
                    shardPath,
                    threadPool
                )
            ) {
                RecoveryState recoveryState = createRecoveryState();
                final boolean loaded = searchableSnapshotDirectory.loadSnapshot(recoveryState);
                assertThat("Failed to load snapshot", loaded, is(true));
                assertThat("Snapshot should be loaded", searchableSnapshotDirectory.snapshot(), notNullValue());
                assertThat("BlobContainer should be loaded", searchableSnapshotDirectory.blobContainer(), notNullValue());

                try (IndexInput indexInput = searchableSnapshotDirectory.openInput(fileName, newIOContext(random()))) {
                    final byte[] buffer = new byte[input.length + 1];
                    final IOException exception = expectThrows(IOException.class, () -> indexInput.readBytes(buffer, 0, buffer.length));
                    if (containsEOFException(exception, new HashSet<>()) == false) {
                        throw new AssertionError("inner EOFException not thrown", exception);
                    }
                }
            } finally {
                terminate(threadPool);
            }
        }
    }

    private boolean containsEOFException(Throwable throwable, HashSet<Throwable> seenThrowables) {
        if (throwable == null || seenThrowables.add(throwable) == false) {
            return false;
        }
        if (throwable instanceof EOFException) {
            return true;
        }
        for (Throwable suppressed : throwable.getSuppressed()) {
            if (containsEOFException(suppressed, seenThrowables)) {
                return true;
            }
        }
        return containsEOFException(throwable.getCause(), seenThrowables);
    }

    private SearchableSnapshotRecoveryState createRecoveryState() {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            randomAlphaOfLength(10),
            0,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING
        );
        DiscoveryNode targetNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        return new SearchableSnapshotRecoveryState(shardRouting, targetNode, null);
    }

    /**
     * BlobContainer that counts the number of {@link java.io.InputStream} it opens, as well as the
     * total number of bytes read from them.
     */
    private static class CountingBlobContainer extends FilterBlobContainer {

        private final LongAdder totalBytes = new LongAdder();
        private final LongAdder totalOpens = new LongAdder();

        private final int rangeSize;

        CountingBlobContainer(BlobContainer in, int rangeSize) {
            super(in);
            this.rangeSize = rangeSize;
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return new CountingInputStream(this, super.readBlob(blobName, position, length), length, rangeSize);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new CountingBlobContainer(child, this.rangeSize);
        }

        @Override
        public InputStream readBlob(String name) {
            assert false : "this method should never be called";
            throw new UnsupportedOperationException();
        }
    }

    /**
     * InputStream that counts the number of bytes read from it, as well as the positions
     * where read operations start and finish.
     */
    private static class CountingInputStream extends FilterInputStream {

        private final CountingBlobContainer container;
        private final int rangeSize;
        private final long length;

        private long bytesRead = 0L;
        private long position = 0L;
        private long start = Long.MAX_VALUE;
        private long end = Long.MIN_VALUE;

        CountingInputStream(CountingBlobContainer container, InputStream input, long length, int rangeSize) {
            super(input);
            this.container = Objects.requireNonNull(container);
            this.rangeSize = rangeSize;
            this.length = length;
            this.container.totalOpens.increment();
        }

        @Override
        public int read() throws IOException {
            if (position < start) {
                start = position;
            }

            final int result = in.read();
            if (result == -1) {
                return result;
            }
            bytesRead += 1L;
            position += 1L;

            if (position > end) {
                end = position;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int offset, int len) throws IOException {
            if (position < start) {
                start = position;
            }

            final int result = in.read(b, offset, len);
            bytesRead += len;
            position += len;

            if (position > end) {
                end = position;
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            in.close();
            if (start % rangeSize != 0) {
                throw new AssertionError("Read operation should start at the beginning of a range");
            }
            if (end % rangeSize != 0) {
                if (end != length) {
                    throw new AssertionError("Read operation should finish at the end of a range or the end of the file");
                }
            }
            if (length <= rangeSize) {
                if (bytesRead != length) {
                    throw new AssertionError("All [" + length + "] bytes should have been read, no more no less but got:" + bytesRead);
                }
            } else {
                if (bytesRead != rangeSize) {
                    if (end != length) {
                        throw new AssertionError("Expecting [" + rangeSize + "] bytes to be read but got:" + bytesRead);

                    }
                    final long remaining = length % rangeSize;
                    if (bytesRead != remaining) {
                        throw new AssertionError("Expecting [" + remaining + "] bytes to be read but got:" + bytesRead);
                    }
                }
            }
            this.container.totalBytes.add(bytesRead);
        }
    }
}
