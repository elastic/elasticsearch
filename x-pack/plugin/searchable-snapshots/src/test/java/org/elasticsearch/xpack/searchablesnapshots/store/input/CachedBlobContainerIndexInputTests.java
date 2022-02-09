/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.NoopBlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.singleBlobContainer;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.singleSplitBlobContainer;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.resolveSnapshotCache;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class CachedBlobContainerIndexInputTests extends AbstractSearchableSnapshotsTestCase {

    public void testRandomReads() throws Exception {
        try (CacheService cacheService = randomCacheService()) {
            cacheService.start();

            SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            IndexId indexId = new IndexId("_name", "_uuid");
            ShardId shardId = new ShardId("_name", "_uuid", 0);

            for (int i = 0; i < 5; i++) {
                final String fileName = randomAlphaOfLength(5) + randomFileExtension();
                final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 100_000));

                final byte[] input = bytes.v2();
                final String checksum = bytes.v1();
                final String blobName = randomUnicodeOfLength(10);
                final StoreFileMetadata metadata = new StoreFileMetadata(
                    fileName,
                    input.length,
                    checksum,
                    Version.CURRENT.luceneVersion.toString()
                );

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
                if (input.length == partSize && prewarmEnabled == false) {
                    blobContainer = new CountingBlobContainer(singleBlobContainer);
                } else {
                    blobContainer = singleBlobContainer;
                }

                final boolean recoveryFinalizedDone = randomBoolean();
                final Path shardDir = randomShardPath(shardId);
                final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
                final Path cacheDir = Files.createDirectories(resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
                final FrozenCacheService frozenCacheService = defaultFrozenCacheService();
                try (
                    SearchableSnapshotDirectory directory = new SearchableSnapshotDirectory(
                        () -> blobContainer,
                        () -> snapshot,
                        new NoopBlobStoreCacheService(),
                        "_repo",
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
                        threadPool,
                        frozenCacheService
                    )
                ) {
                    RecoveryState recoveryState = createRecoveryState(recoveryFinalizedDone);
                    final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                    final boolean loaded = directory.loadSnapshot(recoveryState, future);
                    if (randomBoolean()) {
                        // randomly wait for pre-warm before running the below reads
                        future.get();
                    }
                    assertThat("Failed to load snapshot", loaded, is(true));
                    assertThat("Snapshot should be loaded", directory.snapshot(), notNullValue());
                    assertThat("BlobContainer should be loaded", directory.blobContainer(), notNullValue());

                    try (IndexInput indexInput = directory.openInput(fileName, randomIOContext())) {
                        assertThat(indexInput, instanceOf(CachedBlobContainerIndexInput.class));
                        assertEquals(input.length, indexInput.length());
                        assertEquals(0, indexInput.getFilePointer());
                        byte[] output = randomReadAndSlice(indexInput, input.length);
                        assertArrayEquals(input, output);
                    }
                } finally {
                    frozenCacheService.close();
                }

                if (blobContainer instanceof CountingBlobContainer) {
                    long numberOfRanges = TestUtils.numberOfRanges(
                        input.length,
                        recoveryFinalizedDone ? cacheService.getRangeSize() : cacheService.getRecoveryRangeSize()
                    );
                    assertThat(
                        "Expected at most " + numberOfRanges + " ranges fetched from the source",
                        ((CountingBlobContainer) blobContainer).totalOpens.sum(),
                        lessThanOrEqualTo(numberOfRanges)
                    );
                    assertThat(
                        "All bytes should have been read from source",
                        ((CountingBlobContainer) blobContainer).totalBytes.sum(),
                        equalTo((long) input.length)
                    );
                    // busy assert that closing of all streams happened because they are closed on background fetcher threads
                    assertBusy(
                        () -> assertEquals(
                            "All open streams should have been closed",
                            0,
                            ((CountingBlobContainer) blobContainer).openStreams.get()
                        )
                    );
                }
            }
        } finally {
            assertThreadPoolNotBusy(threadPool);
        }
    }

    public void testThrowsEOFException() throws Exception {
        try (CacheService cacheService = randomCacheService()) {
            cacheService.start();

            SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            IndexId indexId = new IndexId("_name", "_uuid");
            ShardId shardId = new ShardId("_name", "_uuid", 0);

            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 1000));

            final byte[] input = bytes.v2();
            final String checksum = bytes.v1();
            final String blobName = randomUnicodeOfLength(10);
            final StoreFileMetadata metadata = new StoreFileMetadata(
                fileName,
                input.length,
                checksum,
                Version.CURRENT.luceneVersion.toString()
            );

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
            final Path shardDir = randomShardPath(shardId);
            final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
            final Path cacheDir = Files.createDirectories(resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
            final FrozenCacheService frozenCacheService = defaultFrozenCacheService();
            try (
                SearchableSnapshotDirectory searchableSnapshotDirectory = new SearchableSnapshotDirectory(
                    () -> blobContainer,
                    () -> snapshot,
                    new NoopBlobStoreCacheService(),
                    "_repo",
                    snapshotId,
                    indexId,
                    shardId,
                    Settings.EMPTY,
                    () -> 0L,
                    cacheService,
                    cacheDir,
                    shardPath,
                    threadPool,
                    frozenCacheService
                )
            ) {
                RecoveryState recoveryState = createRecoveryState(randomBoolean());
                final PlainActionFuture<Void> f = PlainActionFuture.newFuture();
                final boolean loaded = searchableSnapshotDirectory.loadSnapshot(recoveryState, f);
                try {
                    f.get();
                } catch (ExecutionException e) {
                    assertNotNull(ExceptionsHelper.unwrap(e, IOException.class));
                }
                assertThat("Failed to load snapshot", loaded, is(true));
                assertThat("Snapshot should be loaded", searchableSnapshotDirectory.snapshot(), notNullValue());
                assertThat("BlobContainer should be loaded", searchableSnapshotDirectory.blobContainer(), notNullValue());

                try (IndexInput indexInput = searchableSnapshotDirectory.openInput(fileName, randomIOContext())) {
                    assertThat(indexInput, instanceOf(CachedBlobContainerIndexInput.class));
                    final byte[] buffer = new byte[input.length + 1];
                    final IOException exception = expectThrows(IOException.class, () -> indexInput.readBytes(buffer, 0, buffer.length));
                    if (containsEOFException(exception, new HashSet<>()) == false) {
                        throw new AssertionError("inner EOFException not thrown", exception);
                    }
                }
            } finally {
                frozenCacheService.close();
                assertThreadPoolNotBusy(threadPool);
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

    /**
     * BlobContainer that counts the number of {@link java.io.InputStream} it opens, as well as the
     * total number of bytes read from them.
     */
    private static class CountingBlobContainer extends FilterBlobContainer {

        private final LongAdder totalBytes = new LongAdder();
        private final LongAdder totalOpens = new LongAdder();

        private final AtomicInteger openStreams = new AtomicInteger(0);

        CountingBlobContainer(BlobContainer in) {
            super(in);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return new CountingInputStream(this, super.readBlob(blobName, position, length));
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new CountingBlobContainer(child);
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

        private long position = 0L;
        private long start = Long.MAX_VALUE;
        private long end = Long.MIN_VALUE;

        CountingInputStream(CountingBlobContainer container, InputStream input) {
            super(input);
            this.container = Objects.requireNonNull(container);
            this.container.totalOpens.increment();
            this.container.openStreams.incrementAndGet();
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
            this.container.totalBytes.increment();
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
            this.container.totalBytes.add(len);
            position += len;

            if (position > end) {
                end = position;
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.container.openStreams.decrementAndGet();
        }
    }
}
