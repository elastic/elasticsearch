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
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommitTestUtils;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.lucene.SearchIndexInput;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongFunction;

import static co.elastic.elasticsearch.stateless.lucene.SearchDirectoryTestUtils.getCacheService;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class CacheBlobReaderTests extends ESTestCase {

    @Override
    public String[] tmpPaths() {
        // cache requires a single data path
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    /**
     * A custom {@link FakeStatelessNode} that builds a {@link VirtualBatchedCompoundCommit} with a few commits and its
     * {@link co.elastic.elasticsearch.stateless.lucene.SearchDirectory} uses that to get VBCC chunks from.
     */
    private class FakeVBCCStatelessNode extends FakeStatelessNode {
        protected final VirtualBatchedCompoundCommit virtualBatchedCompoundCommit;
        private final AtomicInteger blobReads = new AtomicInteger(0);

        FakeVBCCStatelessNode(
            Function<Settings, Environment> environmentSupplier,
            CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
            NamedXContentRegistry xContentRegistry,
            long primaryTerm
        ) throws IOException {
            this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm, 0);
        }

        FakeVBCCStatelessNode(
            Function<Settings, Environment> environmentSupplier,
            CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
            NamedXContentRegistry xContentRegistry,
            long primaryTerm,
            long minimumVbccSize
        ) throws IOException {
            super(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm);
            final var indexBlobContainer = indexingDirectory.getSearchDirectory().getBlobContainer(primaryTerm);
            searchDirectory.setBlobContainer((term) -> indexBlobContainer);

            var commits = generateIndexCommits(randomIntBetween(1, 3), randomBoolean());
            virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                shardId,
                "node-id",
                primaryTerm,
                commits.get(0).getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong
            );
            appendCommitsToVbcc(commits);

            while (virtualBatchedCompoundCommit.getTotalSizeInBytes() < minimumVbccSize) {
                var moreCommits = generateIndexCommits(randomIntBetween(1, 30), false);
                appendCommitsToVbcc(moreCommits);
            }
        }

        private void appendCommitsToVbcc(List<StatelessCommitRef> commits) {
            assert virtualBatchedCompoundCommit != null;
            for (StatelessCommitRef statelessCommitRef : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef));
                var pendingCompoundCommits = VirtualBatchedCompoundCommitTestUtils.getPendingStatelessCompoundCommits(
                    virtualBatchedCompoundCommit
                );
                searchDirectory.updateCommit(pendingCompoundCommits.get(pendingCompoundCommits.size() - 1));
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
                SetOnce<BatchedCompoundCommit> bcc = new SetOnce<>();
                var indexBlobContainer = indexingDirectory.getSearchDirectory().getBlobContainer(getPrimaryTerm());
                indexBlobContainer.writeMetadataBlob(
                    OperationPurpose.INDICES,
                    virtualBatchedCompoundCommit.getBlobName(),
                    false,
                    true,
                    out -> {
                        bcc.set(virtualBatchedCompoundCommit.writeToStore(out));
                    }
                );
                virtualBatchedCompoundCommit.decRef();

                SearchDirectoryTestUtils.updateLastUploadedTermAndGen(
                    searchDirectory,
                    virtualBatchedCompoundCommit.getPrimaryTermAndGeneration()
                );
                return Objects.requireNonNull(bcc.get());
            }));
            return future.actionGet(30, TimeUnit.SECONDS);
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
            return new CacheBlobReaderService(nodeSettings, cacheService, client) {
                @Override
                public CacheBlobReader getCacheBlobReader(
                    ShardId shardId,
                    LongFunction<BlobContainer> blobContainer,
                    BlobLocation location,
                    ObjectStoreUploadTracker objectStoreUploadTracker
                ) {
                    var originalCacheBlobReader = originalCacheBlobReaderService.getCacheBlobReader(
                        shardId,
                        blobContainer,
                        location,
                        objectStoreUploadTracker
                    );
                    var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
                        shardId,
                        location.getBatchedCompoundCommitTermAndGeneration(),
                        client,
                        TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings)
                    ) {
                        @Override
                        public void getVirtualBatchedCompoundCommitChunk(
                            final PrimaryTermAndGeneration virtualBccTermAndGen,
                            final long offset,
                            final int length,
                            final ActionListener<ReleasableBytesReference> listener
                        ) {
                            customizedGetVirtualBatchedCompoundCommitChunk(virtualBccTermAndGen, offset, length, listener);
                        }
                    };
                    return new SwitchingCacheBlobReader(
                        location.getBatchedCompoundCommitTermAndGeneration(),
                        objectStoreUploadTracker,
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
                var searchIndexInput = new SearchIndexInput(
                    "fileName",
                    sharedCacheService.getCacheFile(
                        new FileCacheKey(shardId, getPrimaryTerm(), virtualBatchedCompoundCommit.getBlobName()),
                        virtualBatchedCompoundCommit.getTotalSizeInBytes()
                    ),
                    randomIOContext(),
                    cacheBlobReaderService.getCacheBlobReader(
                        shardId,
                        (term) -> indexingDirectory.getSearchDirectory().getBlobContainer(term),
                        virtualBatchedCompoundCommit.getInternalLocations().get(getLastInternalLocation().getKey()),
                        new ObjectStoreUploadTracker() {
                            @Override
                            public boolean isUploaded(PrimaryTermAndGeneration termAndGen) {
                                return false;
                            }
                        }
                    ),
                    1,
                    offset
                )
            ) {
                searchIndexInput.readByte();
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
                    return super.readBlob(purpose, blobName);
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    blobReads.incrementAndGet();
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
            assertThat(input, instanceOf(SearchIndexInput.class));
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

            var bcc = node.uploadVirtualBatchedCompoundCommit();

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

            // Read all files and ensure they are the same. This reads from the indexing node into the cache.
            virtualBatchedCompoundCommit.getInternalLocations().keySet().forEach(filename -> assertFileChecksum(searchDirectory, filename));

            var bcc = node.uploadVirtualBatchedCompoundCommit();

            // Read all files and ensure they are the same. This reads from cache (should be big) without reading from the blob store.
            int blobReadsBefore = node.getBlobReads();
            virtualBatchedCompoundCommit.getInternalLocations().keySet().forEach(filename -> assertFileChecksum(searchDirectory, filename));
            assertThat(node.getBlobReads(), equalTo(blobReadsBefore));
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

    public void testCacheBlobReaderCanCauseSingleRequestedRangeNotSatisfiedException() throws Exception {
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
                Queue<Long> getRangeInputStreamCalls = new ConcurrentLinkedQueue<Long>();

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

                @Override
                protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                    var originalCacheBlobReaderService = super.createCacheBlobReaderService(cacheService);
                    return new CacheBlobReaderService(nodeSettings, cacheService, client) {
                        @Override
                        public CacheBlobReader getCacheBlobReader(
                            ShardId shardId,
                            LongFunction<BlobContainer> blobContainer,
                            BlobLocation location,
                            ObjectStoreUploadTracker objectStoreUploadTracker
                        ) {
                            var originalCacheBlobReader = originalCacheBlobReaderService.getCacheBlobReader(
                                shardId,
                                blobContainer,
                                location,
                                objectStoreUploadTracker
                            );
                            return new CacheBlobReader() {
                                @Override
                                public ByteRange getRange(long position, int length, long remainingFileLength) {
                                    return originalCacheBlobReader.getRange(position, length, remainingFileLength);
                                }

                                @Override
                                public InputStream getRangeInputStream(long position, int length) throws IOException {
                                    assert length > 0;
                                    getRangeInputStreamCalls.add(position);
                                    return originalCacheBlobReader.getRangeInputStream(position, length);
                                }
                            };
                        }
                    };
                }
            }
        ) {
            final var vbccSize = node.virtualBatchedCompoundCommit.getTotalSizeInBytes();

            // Read a byte in the last page of the blob, which makes the cache read the last chunk from the indexing shard, up to the
            // end of the blob.
            long lastPageOffset = vbccSize % PAGE_SIZE == 0 ? (vbccSize / PAGE_SIZE - 1) * PAGE_SIZE : (vbccSize / PAGE_SIZE) * PAGE_SIZE;
            long offsetInLastPage = randomLongBetween(lastPageOffset, vbccSize - 1);
            node.readVirtualBatchedCompoundCommitByte(offsetInLastPage);

            // Upload and switch to read from the object store
            var bcc = node.uploadVirtualBatchedCompoundCommit();

            // Read the files, which makes the cache fill a 16MiB region from the cache that extends beyond the end of the blob file.
            // This means it will need to fill two gaps: one before the chunk we read above, and one that starts at the next page after
            // the end of the blob file up to the end of the region. That last gap results in RequestedRangeNotSatisfiedException, which
            // should be handled and ignored by ObjectStoreCacheBlobReader, so that the gap is not attempted to be fetched again.
            node.virtualBatchedCompoundCommit.getInternalLocations()
                .keySet()
                .forEach(filename -> assertFileChecksum(node.searchDirectory, filename));

            // 3 calls expected for: the chunk from the indexing node, the gap before the chunk, and the gap beyond the end of the blob file
            assertThat(node.getRangeInputStreamCalls.size(), equalTo(3));
            long lastChunkOffset = (offsetInLastPage / chunkSize) * chunkSize;
            assertThat(
                node.getRangeInputStreamCalls.stream().toList(),
                containsInAnyOrder(lastChunkOffset, 0L, BlobCacheUtils.toPageAlignedSize(vbccSize))
            );
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1915")
    public void testCacheBlobReaderSwitchesFromIndexingToBlobStoreOnIntermediateUpload() throws Exception {
        final var primaryTerm = randomLongBetween(1L, 10L);
        try (var node = new FakeVBCCStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                var originalCacheBlobReaderService = super.createCacheBlobReaderService(cacheService);
                return new CacheBlobReaderService(nodeSettings, cacheService, client) {
                    @Override
                    public CacheBlobReader getCacheBlobReader(
                        ShardId shardId,
                        LongFunction<BlobContainer> blobContainer,
                        BlobLocation location,
                        ObjectStoreUploadTracker objectStoreUploadTracker
                    ) {
                        var originalCacheBlobReader = originalCacheBlobReaderService.getCacheBlobReader(
                            shardId,
                            blobContainer,
                            location,
                            objectStoreUploadTracker
                        );
                        var writerFromPrimary = new IndexingShardCacheBlobReader(
                            shardId,
                            location.getBatchedCompoundCommitTermAndGeneration(),
                            client,
                            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings)
                        ) {
                            @Override
                            public void getVirtualBatchedCompoundCommitChunk(
                                final PrimaryTermAndGeneration virtualBccTermAndGen,
                                final long offset,
                                final int length,
                                final ActionListener<ReleasableBytesReference> listener
                            ) {
                                if (randomBoolean()) {
                                    customizedGetVirtualBatchedCompoundCommitChunk(virtualBccTermAndGen, offset, length, listener);
                                } else {
                                    try {
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
                            location.getBatchedCompoundCommitTermAndGeneration(),
                            objectStoreUploadTracker,
                            originalCacheBlobReader,
                            writerFromPrimary
                        );
                    }
                };
            }
        }) {
            var virtualBatchedCompoundCommit = node.virtualBatchedCompoundCommit;
            var searchDirectory = node.searchDirectory;

            // Read all files and ensure they are the same. This tries to read from the VBCC, but it uploads it in the meantime and
            // produces a ResourceNotFoundException that makes the search directory read from the blob store ultimately.
            virtualBatchedCompoundCommit.getInternalLocations().keySet().forEach(filename -> assertFileChecksum(searchDirectory, filename));
        }
    }

}
