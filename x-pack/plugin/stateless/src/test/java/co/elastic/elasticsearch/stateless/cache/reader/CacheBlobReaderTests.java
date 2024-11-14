/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.cache.reader;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommitTestUtils;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.BlobCacheIndexInput;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CountingFilterInputStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL;
import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class CacheBlobReaderTests extends ESTestCase {

    @Override
    public String[] tmpPaths() {
        // cache requires a single data path
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    record GetRangeInputStreamCall(long position, int length, IntSupplier bytesReadSupplier) {}

    /**
     * A custom {@link FakeStatelessNode} that builds a {@link VirtualBatchedCompoundCommit} with a few commits and its
     * {@link co.elastic.elasticsearch.stateless.lucene.SearchDirectory} uses that to get VBCC chunks from.
     */
    private class FakeVBCCStatelessNode extends FakeStatelessNode {
        protected final VirtualBatchedCompoundCommit virtualBatchedCompoundCommit;
        private final AtomicInteger blobReads = new AtomicInteger(0);
        final Queue<GetRangeInputStreamCall> getRangeInputStreamCalls = new ConcurrentLinkedQueue<>();

        FakeVBCCStatelessNode(
            Function<Settings, Environment> environmentSupplier,
            CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
            NamedXContentRegistry xContentRegistry,
            long primaryTerm
        ) throws IOException {
            this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm, 0, randomBoolean());
        }

        FakeVBCCStatelessNode(
            Function<Settings, Environment> environmentSupplier,
            CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
            NamedXContentRegistry xContentRegistry,
            long primaryTerm,
            long minimumVbccSize
        ) throws IOException {
            this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm, minimumVbccSize, randomBoolean());
        }

        FakeVBCCStatelessNode(
            Function<Settings, Environment> environmentSupplier,
            CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
            NamedXContentRegistry xContentRegistry,
            long primaryTerm,
            long minimumVbccSize,
            boolean useInternalFilesReplicatedContent
        ) throws IOException {
            super(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm);

            var commits = generateIndexCommits(randomIntBetween(1, 3), randomBoolean());
            virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                shardId,
                "node-id",
                primaryTerm,
                commits.get(0).getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                sharedCacheService.getRegionSize(),
                randomDoubleBetween(0.0d, 1.0d, true)
            );
            appendCommitsToVbcc(commits, useInternalFilesReplicatedContent);

            while (virtualBatchedCompoundCommit.getTotalSizeInBytes() < minimumVbccSize) {
                var moreCommits = generateIndexCommits(randomIntBetween(1, 30), false);
                appendCommitsToVbcc(moreCommits, useInternalFilesReplicatedContent);
            }
        }

        private void appendCommitsToVbcc(List<StatelessCommitRef> commits, boolean useInternalFilesReplicatedContent) {
            assert virtualBatchedCompoundCommit != null;
            for (StatelessCommitRef statelessCommitRef : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, useInternalFilesReplicatedContent));
                var pendingCompoundCommits = VirtualBatchedCompoundCommitTestUtils.getPendingStatelessCompoundCommits(
                    virtualBatchedCompoundCommit
                );
                StatelessCompoundCommit latestCommit = pendingCompoundCommits.get(pendingCompoundCommits.size() - 1);
                logger.debug("Updating Search directory with CC: {}", latestCommit.toLongDescription());
                searchDirectory.updateCommit(latestCommit);
            }
        }

        public VirtualBatchedCompoundCommit getVirtualBatchedCompoundCommit() {
            return virtualBatchedCompoundCommit;
        }

        public Map.Entry<String, BlobLocation> getLastInternalLocation() {
            // Get the last internal file of the VBCC. We do that by using a comparator that compares their offsets, and use that
            // comparator to get the max of the VBCC internal locations.
            return virtualBatchedCompoundCommit.getInternalLocations().entrySet().stream().max((l, r) -> {
                return Math.toIntExact(l.getValue().offset() - r.getValue().offset());
            }).get();
        }

        public synchronized BatchedCompoundCommit uploadVirtualBatchedCompoundCommit() throws IOException {
            PlainActionFuture<BatchedCompoundCommit> future = new PlainActionFuture<>();
            // move to a thread pool that allows reading data from vbcc.
            threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL).execute(ActionRunnable.supply(future, () -> {
                if (virtualBatchedCompoundCommit.freeze() == false) {
                    return null;
                }

                // Upload vBCC to blob store
                var indexBlobContainer = indexingDirectory.getBlobStoreCacheDirectory().getBlobContainer(getPrimaryTerm());
                try (var vbccInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                    indexBlobContainer.writeBlobAtomic(
                        OperationPurpose.INDICES,
                        virtualBatchedCompoundCommit.getBlobName(),
                        vbccInputStream,
                        virtualBatchedCompoundCommit.getTotalSizeInBytes(),
                        false
                    );
                }
                BatchedCompoundCommit bcc = virtualBatchedCompoundCommit.getFrozenBatchedCompoundCommit();
                virtualBatchedCompoundCommit.decRef();
                return Objects.requireNonNull(bcc);
            }));
            return future.actionGet(30, TimeUnit.SECONDS);
        }

        public synchronized BatchedCompoundCommit uploadVirtualBatchedCompoundCommitAndNotifySearchDirectory() throws IOException {
            var bcc = uploadVirtualBatchedCompoundCommit();
            BlobStoreCacheDirectoryTestUtils.updateLatestUploadedBcc(searchDirectory, bcc.primaryTermAndGeneration());
            BlobStoreCacheDirectoryTestUtils.updateLatestCommitInfo(
                searchDirectory,
                bcc.lastCompoundCommit().primaryTermAndGeneration(),
                clusterService.localNode().getId()
            );
            return bcc;
        }

        @Override
        protected Settings nodeSettings() {
            return Settings.builder()
                .put(super.nodeSettings())
                .put(
                    SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    rarely()
                        ? randomBoolean()
                            ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.KB).getStringRep()
                            : new ByteSizeValue(randomIntBetween(1, 1000), ByteSizeUnit.BYTES).getStringRep()
                        : randomBoolean() ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB).getStringRep()
                        // only use up to 0.1% disk to be friendly.
                        : new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString()
                )
                .build();
        }

        @Override
        protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
            var originalCacheBlobReaderService = super.createCacheBlobReaderService(cacheService);
            return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                @Override
                public CacheBlobReader getCacheBlobReader(
                    ShardId shardId,
                    LongFunction<BlobContainer> blobContainer,
                    BlobLocation location,
                    MutableObjectStoreUploadTracker objectStoreUploadTracker,
                    LongConsumer totalBytesReadFromObjectStore,
                    LongConsumer totalBytesReadFromIndexing,
                    BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                    Executor objectStoreFetchExecutor
                ) {
                    var originalCacheBlobReader = originalCacheBlobReaderService.getCacheBlobReader(
                        shardId,
                        blobContainer,
                        location,
                        objectStoreUploadTracker,
                        totalBytesReadFromObjectStore,
                        totalBytesReadFromIndexing,
                        cachePopulationReason,
                        objectStoreFetchExecutor
                    );
                    var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
                        shardId,
                        location.getBatchedCompoundCommitTermAndGeneration(),
                        clusterService.localNode().getId(),
                        client,
                        TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings),
                        threadPool
                    ) {
                        @Override
                        public void getVirtualBatchedCompoundCommitChunk(
                            final PrimaryTermAndGeneration virtualBccTermAndGen,
                            final long offset,
                            final int length,
                            final String preferredNodeId,
                            final ActionListener<ReleasableBytesReference> listener
                        ) {
                            customizedGetVirtualBatchedCompoundCommitChunk(virtualBccTermAndGen, offset, length, listener);
                        }
                    };
                    return new SwitchingCacheBlobReader(
                        objectStoreUploadTracker,
                        location.getBatchedCompoundCommitTermAndGeneration(),
                        originalCacheBlobReader,
                        indexingShardCacheBlobReader
                    );
                }
            };
        }

        protected void customizedGetVirtualBatchedCompoundCommitChunk(
            final PrimaryTermAndGeneration virtualBccTermAndGen,
            final long offset,
            final int length,
            final ActionListener<ReleasableBytesReference> listener
        ) {
            assertEquals(virtualBatchedCompoundCommit.getPrimaryTermAndGeneration(), virtualBccTermAndGen);
            int finalLength = Math.min(length, Math.toIntExact(virtualBatchedCompoundCommit.getTotalSizeInBytes() - offset));
            var bytesStreamOutput = new BytesStreamOutput(finalLength);
            threadPool.executor(Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL).execute(() -> {
                ActionListener.run(listener, l -> {
                    virtualBatchedCompoundCommit.getBytesByRange(offset, finalLength, bytesStreamOutput);
                    l.onResponse(new ReleasableBytesReference(bytesStreamOutput.bytes(), bytesStreamOutput::close));
                });
            });
        }

        public void readVirtualBatchedCompoundCommitByte(long offset) {
            try (
                var blobCacheIndexInput = new BlobCacheIndexInput(
                    "fileName",
                    randomIOContext(),
                    new CacheFileReader(
                        sharedCacheService.getCacheFile(
                            new FileCacheKey(shardId, getPrimaryTerm(), virtualBatchedCompoundCommit.getBlobName()),
                            virtualBatchedCompoundCommit.getTotalSizeInBytes()
                        ),
                        cacheBlobReaderService.getCacheBlobReader(
                            shardId,
                            (term) -> indexingDirectory.getBlobStoreCacheDirectory().getBlobContainer(term),
                            virtualBatchedCompoundCommit.getInternalLocations().get(getLastInternalLocation().getKey()),
                            new MutableObjectStoreUploadTracker() {
                                @Override
                                public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
                                    assert false : "unexpected call to this method";
                                }

                                @Override
                                public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
                                    assert false : "unexpected call to this method";
                                }

                                @Override
                                public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
                                    return new ObjectStoreUploadTracker.UploadInfo() {
                                        @Override
                                        public boolean isUploaded() {
                                            return false;
                                        }

                                        @Override
                                        public String preferredNodeId() {
                                            return null;
                                        }
                                    };
                                }
                            },
                            bytesReadFromObjectStore -> {},
                            bytesReadFromIndexing -> {},
                            BlobCacheMetrics.CachePopulationReason.CacheMiss,
                            threadPool.executor(SHARD_READ_THREAD_POOL)
                        ),
                        new BlobFileRanges(getLastInternalLocation().getValue())
                    ),
                    null,
                    1,
                    offset
                )
            ) {
                blobCacheIndexInput.readByte();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
            return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                    blobReads.incrementAndGet();
                    if (blobName.contains(StatelessCompoundCommit.PREFIX)) {
                        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                    }
                    logger.debug("reading {} from blob store", blobName);
                    return super.readBlob(purpose, blobName);
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    blobReads.incrementAndGet();
                    if (blobName.contains(StatelessCompoundCommit.PREFIX)) {
                        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                    }
                    logger.debug("reading {} from blob store, position: {} length: {}", blobName, position, length);
                    return super.readBlob(purpose, blobName, position, length);
                }
            };
        }

        public int getBlobReads() {
            return blobReads.get();
        }
    }

    private long assertFileChecksum(Directory directory, String filename) {
        try (var input = directory.openInput(filename, IOContext.DEFAULT)) {
            assertThat(input, instanceOf(BlobCacheIndexInput.class));
            return CodecUtil.checksumEntireFile(input);
        } catch (IOException e) {
            logger.error("Unexpected exception while reading from BCC", e);
            assert false : e;
        }
        return 0; // cannot happen
    }

    public void testCacheBlobReaderFetchFromIndexingAndSwitchToBlobStore() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        try (var node = new FakeVBCCStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm)) {
            var virtualBatchedCompoundCommit = node.virtualBatchedCompoundCommit;
            var searchDirectory = node.searchDirectory;

            // Read all files and ensure they are the same. This reads from the indexing node into the cache.
            int blobReadsBefore = node.getBlobReads();
            var checksums = new HashMap<String, Long>(virtualBatchedCompoundCommit.getInternalLocations().keySet().size());
            virtualBatchedCompoundCommit.getInternalLocations().keySet().forEach(filename -> {
                long checksum = assertFileChecksum(searchDirectory, filename);
                checksums.put(filename, checksum);
            });
            assertThat(node.getBlobReads(), equalTo(blobReadsBefore));

            var bcc = node.uploadVirtualBatchedCompoundCommitAndNotifySearchDirectory();

            // Read all files and ensure they are the same. This reads from cache (if big enough) without reading from the blob store.
            virtualBatchedCompoundCommit.getInternalLocations().keySet().forEach(filename -> {
                long checksum = assertFileChecksum(searchDirectory, filename);
                assertThat(checksum, equalTo(checksums.get(filename)));
            });

            // Invalidate cache
            getCacheService(searchDirectory).forceEvict((key) -> true);

            // Read all files and ensure they are the same. This reads from the blob store into the cache.
            blobReadsBefore = node.getBlobReads();
            bcc.compoundCommits().forEach(cc -> {
                cc.commitFiles().forEach((filename, blobLocation) -> {
                    long checksum = assertFileChecksum(searchDirectory, filename);
                    assertThat(checksum, equalTo(checksums.get(filename)));
                });
            });
            assertThat(node.getBlobReads(), greaterThan(blobReadsBefore));
        }
    }

    public void testCacheBlobReaderFetchFromIndexingAndNeverFromBlobStore() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        try (var node = new FakeVBCCStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(40, ByteSizeUnit.MB).getStringRep())
                    .build();
            }
        }) {
            var virtualBatchedCompoundCommit = node.virtualBatchedCompoundCommit;
            var searchDirectory = node.searchDirectory;

            var internalFileNames = new ArrayList<>(virtualBatchedCompoundCommit.getInternalLocations().keySet());

            // Read all files and ensure they are the same. This reads from the indexing node into the cache.
            internalFileNames.forEach(filename -> assertFileChecksum(searchDirectory, filename));

            // ensure that all ranges are resolved/filled in cache
            assertBusy(() -> {
                var stats = node.threadPool.stats()
                    .stats()
                    .stream()
                    .filter((s) -> s.name().equals(SHARD_READ_THREAD_POOL))
                    .findFirst()
                    .get();
                assertThat(stats.active(), equalTo(0));
                assertThat(stats.queue(), equalTo(0));
            });

            node.uploadVirtualBatchedCompoundCommitAndNotifySearchDirectory();

            // Read all files and ensure they are the same. This reads from cache (should be big) without reading from the blob store.
            int blobReadsBefore = node.getBlobReads();
            assertThat("All reads should be done from indexing node blob reader", blobReadsBefore, equalTo(0));

            if (randomBoolean()) {
                // access files in different order
                Collections.shuffle(internalFileNames, random());
            }

            internalFileNames.forEach(filename -> assertFileChecksum(searchDirectory, filename));

            int blobReadsAfter = node.getBlobReads();
            assertThat("All reads should be done from the cache", blobReadsAfter, equalTo(0));
        }
    }

    public void testIndexingShardCacheBlobReaderConsidersLastFileWithoutPadding() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        try (var node = new FakeVBCCStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(40, ByteSizeUnit.MB).getStringRep())
                    .build();
            }
        }) {
            assertFileChecksum(node.searchDirectory, node.getLastInternalLocation().getKey());
        }
    }

    public void testTransportBlobReaderChunkSettingByReadingOneChunk() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        final var chunkSize = PAGE_SIZE * randomIntBetween(1, 256); // 4KiB to 1MiB
        try (
            var node = new FakeVBCCStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                chunkSize * 2
            ) {
                @Override
                protected Settings nodeSettings() {
                    return Settings.builder()
                        .put(super.nodeSettings())
                        .put(
                            SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                            new ByteSizeValue(40, ByteSizeUnit.MB).getStringRep()
                        )
                        .put(
                            CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.getKey(),
                            ByteSizeValue.ofBytes(chunkSize).getStringRep()
                        )
                        .build();
                }
            }
        ) {
            var lastEntry = node.getLastInternalLocation();

            // Manually calculate range for the last file
            long start = (lastEntry.getValue().offset() / chunkSize) * chunkSize;
            long end = BlobCacheUtils.toPageAlignedSize(lastEntry.getValue().offset() + lastEntry.getValue().fileLength());
            long expectedBytesToWrite = end - start;

            long writtenBytesBefore = getCacheService(node.searchDirectory).getStats().writeBytes();
            assertFileChecksum(node.searchDirectory, lastEntry.getKey());
            long writtenBytesAfter = getCacheService(node.searchDirectory).getStats().writeBytes();
            long writtenBytes = writtenBytesAfter - writtenBytesBefore;
            assertThat(writtenBytes, equalTo(expectedBytesToWrite));
        }
    }

    public void testTransportBlobReaderChunkSettingByReadingAllChunks() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        final var chunkSize = PAGE_SIZE * randomIntBetween(1, 256); // 4KiB to 1MiB
        try (
            var node = new FakeVBCCStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                chunkSize * 2
            ) {
                @Override
                protected Settings nodeSettings() {
                    return Settings.builder()
                        .put(super.nodeSettings())
                        .put(
                            SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                            new ByteSizeValue(40, ByteSizeUnit.MB).getStringRep()
                        )
                        .put(
                            CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.getKey(),
                            ByteSizeValue.ofBytes(chunkSize).getStringRep()
                        )
                        .build();
                }
            }
        ) {
            final var vbccSize = node.virtualBatchedCompoundCommit.getTotalSizeInBytes();

            // Read last byte of every chunk
            long writtenBytesBefore = getCacheService(node.searchDirectory).getStats().writeBytes();
            for (long offset = chunkSize - 1; offset < vbccSize; offset += chunkSize) {
                node.readVirtualBatchedCompoundCommitByte(offset);
            }
            // Read last byte of VBCC
            node.readVirtualBatchedCompoundCommitByte(vbccSize - 1);
            long writtenBytesAfter = getCacheService(node.searchDirectory).getStats().writeBytes();
            long writtenBytes = writtenBytesAfter - writtenBytesBefore;
            assertThat(writtenBytes, greaterThan(0L));

            // Now go through all files, and ensure nothing is written to the cache (since it was fully populated from the chunks before)
            writtenBytesBefore = getCacheService(node.searchDirectory).getStats().writeBytes();
            node.virtualBatchedCompoundCommit.getInternalLocations()
                .keySet()
                .forEach(filename -> assertFileChecksum(node.searchDirectory, filename));
            writtenBytesAfter = getCacheService(node.searchDirectory).getStats().writeBytes();
            writtenBytes = writtenBytesAfter - writtenBytesBefore;
            assertThat(writtenBytes, equalTo(0L));
        }
    }

    public void testCacheBlobReaderCanFillMultipleGapsWithASingleRequest() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        final var chunkSize = PAGE_SIZE * randomIntBetween(1, 256); // 4KiB to 1MiB
        try (var node = createInputStreamCallTrackingNode(primaryTerm, chunkSize, randomBoolean())) {
            final var vbccSize = node.virtualBatchedCompoundCommit.getTotalSizeInBytes();

            // Read a byte in the last page of the blob, which makes the cache read the last chunk from the indexing shard, up to the
            // end of the blob.
            long lastPageOffset = vbccSize % PAGE_SIZE == 0 ? (vbccSize / PAGE_SIZE - 1) * PAGE_SIZE : (vbccSize / PAGE_SIZE) * PAGE_SIZE;
            long offsetInLastPage = randomLongBetween(lastPageOffset, vbccSize - 1);
            node.readVirtualBatchedCompoundCommitByte(offsetInLastPage);
            // One call expected for the chunk from the indexing node
            long lastChunkOffset = (offsetInLastPage / chunkSize) * chunkSize;
            assertThat(node.getRangeInputStreamCalls.size(), equalTo(1));
            assertThat(node.getRangeInputStreamCalls.poll().position, equalTo(lastChunkOffset));

            // Upload and switch to read from the object store
            node.uploadVirtualBatchedCompoundCommitAndNotifySearchDirectory();
            node.getRangeInputStreamCalls.clear();

            // Read the files, which makes the cache fill a 16MiB region from the cache that extends beyond the end of the blob file.
            // This means it will need to fill two gaps: one before the chunk we read above, and one that starts at the next page after
            // the end of the blob file up to the end of the region. Data for the two gaps will be fetched with a single request that
            // covers the entire region. Since the actual data is less than the region size, the cache will be filled up to the available
            // data. The gaps are always completed which means they should not be fetched again as long as the cache is not evicted.
            node.virtualBatchedCompoundCommit.getInternalLocations()
                .keySet()
                .forEach(filename -> assertFileChecksum(node.searchDirectory, filename));

            // One call expected for the gap before the chunk from the indexing node and the gap beyond the end of the blob file
            assertThat(node.getRangeInputStreamCalls.size(), equalTo(1));
            assertThat(node.getRangeInputStreamCalls.poll().position, equalTo(0L));

            // Read the files again and they should be from the cache and no request should be made
            node.getRangeInputStreamCalls.clear();
            node.virtualBatchedCompoundCommit.getInternalLocations()
                .keySet()
                .forEach(filename -> assertFileChecksum(node.searchDirectory, filename));
            assertThat(node.getRangeInputStreamCalls, empty());
        }
    }

    public void testCacheBlobReaderDoesNotFillGapBeyondTheEndOfAvailableData() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        final var chunkSize = PAGE_SIZE * randomIntBetween(1, 256); // 4KiB to 1MiB
        // TODO (ES-9344) once reader is implemented, read all the gaps using exposed metadata and run with the feature flag enabled
        try (var node = createInputStreamCallTrackingNode(primaryTerm, chunkSize, false)) {
            final int regionSize = node.sharedCacheService.getRegionSize();
            final long vbccSize = node.virtualBatchedCompoundCommit.getTotalSizeInBytes();
            assertThat(vbccSize, lessThan((long) regionSize));

            // Read all the files so that the only gap is the end of the region
            node.virtualBatchedCompoundCommit.getInternalLocations()
                .keySet()
                .forEach(filename -> assertFileChecksum(node.searchDirectory, filename));

            assertThat(node.getRangeInputStreamCalls, not(empty()));
            assertThat(
                "Region is not fully filled",
                node.getRangeInputStreamCalls.stream().mapToLong(e -> e.position + e.length).max().getAsLong(),
                lessThan((long) regionSize)
            );

            // Upload and switch to read from the object store
            node.uploadVirtualBatchedCompoundCommitAndNotifySearchDirectory();

            // Read the files again. The data is all from the cache and there will be no request trying to fill
            // the gap beyond the available data
            node.getRangeInputStreamCalls.clear();
            node.virtualBatchedCompoundCommit.getInternalLocations()
                .keySet()
                .forEach(filename -> assertFileChecksum(node.searchDirectory, filename));
            assertThat(node.getRangeInputStreamCalls, empty());

            // Lucene won't read beyond available data length. But we can force it to do so by manually creating a
            // SearchInputIndex that targets the whole region. This is to test that the read request with position
            // beyond the available data length is properly handled by ObjectStoreCacheBlobReader#getRangeInputStream
            // which returns a nullInputStream for RequestedRangeNotSatisfiedException.
            final var internalLocation = randomFrom(node.virtualBatchedCompoundCommit.getInternalLocations().entrySet());
            final FileCacheKey fileCacheKey = new FileCacheKey(
                node.shardId,
                internalLocation.getValue().primaryTerm(),
                internalLocation.getValue().blobName()
            );
            final var cacheFile = node.sharedCacheService.getCacheFile(fileCacheKey, regionSize);
            final var cacheBlobReader = node.searchDirectory.getCacheBlobReader(internalLocation.getValue());
            final var cacheFileReader = new CacheFileReader(cacheFile, cacheBlobReader, new BlobFileRanges(internalLocation.getValue()));
            final long availableDataLength = BlobCacheUtils.toPageAlignedSize(vbccSize);
            try (var searchInput = new BlobCacheIndexInput("region", IOContext.DEFAULT, cacheFileReader, null, regionSize, 0)) {
                // Read a byte beyond the available data length will trigger the last gap to be filled
                searchInput.readByte(randomLongBetween(availableDataLength, regionSize - 1L));
                assertBusy(() -> {
                    assertThat(node.getRangeInputStreamCalls.size(), equalTo(1));
                    final GetRangeInputStreamCall getRangeInputStreamCall = node.getRangeInputStreamCalls.poll();
                    assertThat(getRangeInputStreamCall.position, equalTo(availableDataLength));
                    assertThat(getRangeInputStreamCall.length, equalTo(regionSize - (int) availableDataLength));
                    assertThat(getRangeInputStreamCall.bytesReadSupplier.getAsInt(), equalTo(0)); // no actual data from source
                });
            }
        }
    }

    private FakeVBCCStatelessNode createInputStreamCallTrackingNode(
        long primaryTerm,
        int chunkSize,
        boolean useInternalFilesReplicatedContent
    ) throws IOException {
        return new FakeVBCCStatelessNode(
            this::newEnvironment,
            this::newNodeEnvironment,
            xContentRegistry(),
            primaryTerm,
            chunkSize * 2,
            useInternalFilesReplicatedContent
        ) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(40, ByteSizeUnit.MB).getStringRep())
                    .put(
                        CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.getKey(),
                        ByteSizeValue.ofBytes(chunkSize).getStringRep()
                    )
                    .build();
            }

            @Override
            protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                var originalCacheBlobReaderService = super.createCacheBlobReaderService(cacheService);
                return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                    @Override
                    public CacheBlobReader getCacheBlobReader(
                        ShardId shardId,
                        LongFunction<BlobContainer> blobContainer,
                        BlobLocation location,
                        MutableObjectStoreUploadTracker objectStoreUploadTracker,
                        LongConsumer totalBytesReadFromObjectStore,
                        LongConsumer totalBytesReadFromIndexing,
                        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                        Executor objectStoreFetchExecutor
                    ) {
                        var originalCacheBlobReader = originalCacheBlobReaderService.getCacheBlobReader(
                            shardId,
                            blobContainer,
                            location,
                            objectStoreUploadTracker,
                            totalBytesReadFromObjectStore,
                            totalBytesReadFromIndexing,
                            cachePopulationReason,
                            objectStoreFetchExecutor
                        );
                        return new CacheBlobReader() {
                            @Override
                            public ByteRange getRange(long position, int length, long remainingFileLength) {
                                return originalCacheBlobReader.getRange(position, length, remainingFileLength);
                            }

                            @Override
                            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                                assert length > 0;
                                originalCacheBlobReader.getRangeInputStream(position, length, listener.map(in -> {
                                    var bytesCountingStream = new CountingFilterInputStream(in);
                                    getRangeInputStreamCalls.add(
                                        new GetRangeInputStreamCall(position, length, bytesCountingStream::getBytesRead)
                                    );
                                    return bytesCountingStream;
                                }));
                            }
                        };
                    }
                };
            }
        };
    }

    public void testCacheBlobReaderSwitchesFromIndexingToBlobStoreOnIntermediateUpload() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        final var rangeSize = new ByteSizeValue(4, ByteSizeUnit.MB);
        try (
            var node = new FakeVBCCStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                new ByteSizeValue(2, ByteSizeUnit.MB).getBytes()
            ) {
                @Override
                protected Settings nodeSettings() {
                    return Settings.builder()
                        .put(super.nodeSettings())
                        .put(
                            SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                            new ByteSizeValue(12, ByteSizeUnit.MB).getStringRep()
                        )
                        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), rangeSize.getStringRep())
                        .build();
                }

                @Override
                protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                    return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                        @Override
                        public CacheBlobReader getCacheBlobReader(
                            ShardId shardId,
                            LongFunction<BlobContainer> blobContainer,
                            BlobLocation location,
                            MutableObjectStoreUploadTracker objectStoreUploadTracker,
                            LongConsumer totalBytesReadFromObjectStore,
                            LongConsumer totalBytesReadFromIndexing,
                            BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                            Executor objectStoreFetchExecutor
                        ) {
                            var writerFromObjectStore = new ObjectStoreCacheBlobReader(
                                blobContainer.apply(location.primaryTerm()),
                                location.blobName(),
                                rangeSize.getBytes(),
                                objectStoreFetchExecutor
                            ) {
                                @Override
                                public ByteRange getRange(long position, int length, long remainingFileLength) {
                                    assert virtualBatchedCompoundCommit.isFrozen()
                                        : "should not call this cache blob reader when the BCC is not uploaded";
                                    var range = super.getRange(position, length, remainingFileLength);
                                    assertThat(range, equalTo(BlobCacheUtils.computeRange(rangeSize.getBytes(), position, length)));
                                    return range;
                                }
                            };
                            var writerFromPrimary = new IndexingShardCacheBlobReader(
                                shardId,
                                location.getBatchedCompoundCommitTermAndGeneration(),
                                clusterService.localNode().getId(),
                                client,
                                TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings),
                                threadPool
                            ) {
                                @Override
                                public void getVirtualBatchedCompoundCommitChunk(
                                    final PrimaryTermAndGeneration virtualBccTermAndGen,
                                    final long offset,
                                    final int length,
                                    final String preferredNodeId,
                                    final ActionListener<ReleasableBytesReference> listener
                                ) {
                                    if (randomBoolean()) {
                                        customizedGetVirtualBatchedCompoundCommitChunk(virtualBccTermAndGen, offset, length, listener);
                                    } else {
                                        try {
                                            // Upload the VBCC, without notifying the search directory of what is the latest uploaded BCC.
                                            // The ResourceNotFoundException should prompt the SwitchingCacheBlobReader to update the info
                                            // that the VBCC was uploaded and the search directory should read from the blob store.
                                            uploadVirtualBatchedCompoundCommit();
                                        } catch (IOException e) {
                                            logger.error("Failed to upload virtual batched compound commit", e);
                                            assert false : e;
                                        }
                                        listener.onFailure(
                                            VirtualBatchedCompoundCommit.buildResourceNotFoundException(shardId, virtualBccTermAndGen)
                                        );
                                    }
                                }
                            };
                            return new SwitchingCacheBlobReader(
                                objectStoreUploadTracker,
                                location.getBatchedCompoundCommitTermAndGeneration(),
                                writerFromObjectStore,
                                writerFromPrimary
                            ) {
                                @Override
                                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                                    // Assert that it's the test thread trying to fetch and not some executor thread.
                                    String threadName = Thread.currentThread().getName();
                                    assert (threadName.startsWith("TEST-") || threadName.startsWith("LuceneTestCase")) : threadName;
                                    super.getRangeInputStream(position, length, listener);
                                }
                            };
                        }
                    };
                }
            }
        ) {
            var virtualBatchedCompoundCommit = node.virtualBatchedCompoundCommit;
            var searchDirectory = node.searchDirectory;

            // Read all files and ensure they are the same. This tries to read from the VBCC, but it uploads it in the meantime and
            // produces a ResourceNotFoundException that makes the search directory read from the blob store ultimately.
            var files = new ArrayList<>(virtualBatchedCompoundCommit.getInternalLocations().keySet());
            if (randomBoolean()) {
                Collections.shuffle(files, getRandom());
            } else {
                Collections.sort(files, Comparator.comparingLong(f -> virtualBatchedCompoundCommit.getInternalLocations().get(f).offset()));
            }

            final Thread[] threads = new Thread[Math.min(randomIntBetween(1, files.size()), Runtime.getRuntime().availableProcessors())];
            logger.info("--> creating {} threads to assert {} file checksums", threads.length, files.size());
            for (int t = 0; t < threads.length; t++) {
                final int thread = t;
                threads[t] = new Thread(() -> {
                    for (int j = thread; j < files.size(); j += threads.length) {
                        assertFileChecksum(searchDirectory, files.get(j));
                    }
                });
                threads[t].setName("TEST-assert-file-T#" + t);
                threads[t].start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }
    }
}
