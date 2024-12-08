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
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchShardSizeCollector;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizesPublisher;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MeteringCacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreCacheBlobReader;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.SearchEngineTestUtils;
import co.elastic.elasticsearch.stateless.engine.ThreadPoolMergeScheduler;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsClient;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.util.Map.entry;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GenerationalDocValuesIT extends AbstractStatelessIntegTestCase {

    /**
     * Plugin to track Lucene opened generational files.
     */
    public static class GenerationalFilesTrackingStatelessPlugin extends Stateless {
        static final AtomicReference<MergeFinder> mergeFinderRef = new AtomicReference<>(MergeFinder.EMPTY);
        static final AtomicBoolean useCustomMergePolicy = new AtomicBoolean(false);

        public GenerationalFilesTrackingStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof GenerationalFilesTrackingStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected IndexBlobStoreCacheDirectory createIndexBlobStoreCacheDirectory(
            StatelessSharedBlobCacheService cacheService,
            ShardId shardId
        ) {
            return new GenerationalFilesTrackingIndexBlobStoreCacheDirectory(cacheService, shardId);
        }

        @Override
        protected SearchDirectory createSearchDirectory(
            StatelessSharedBlobCacheService cacheService,
            CacheBlobReaderService cacheBlobReaderService,
            MutableObjectStoreUploadTracker objectStoreUploadTracker,
            ShardId shardId
        ) {
            return new GenerationalFilesTrackingSearchDirectory(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new GenerationalFilesTrackingStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected SearchShardSizeCollector createSearchShardSizeCollector(
            ClusterSettings clusterSettings,
            ThreadPool threadPool,
            Client client
        ) {
            // We have to disable the search shard size collector in this test suite as some test checks that
            // the search node has a particular set of commits open and the shard size collector might hold a
            // commit for too long breaking such assertions.
            return new SearchShardSizeCollector(clusterSettings, threadPool, new ShardSizeStatsClient(client) {
                @Override
                public void getAllShardSizes(TimeValue boostWindowInterval, ActionListener<Map<ShardId, ShardSize>> listener) {
                    ActionListener.completeWith(listener, Map::of);
                }

                @Override
                public void getShardSize(ShardId shardId, TimeValue boostWindowInterval, ActionListener<ShardSize> listener) {
                    ActionListener.completeWith(listener, () -> null);
                }
            }, new ShardSizesPublisher(client));
        }

        @Override
        protected MergePolicy getMergePolicy(EngineConfig engineConfig) {
            return new FilterMergePolicy(super.getMergePolicy(engineConfig)) {
                @Override
                public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
                    throws IOException {
                    if (useCustomMergePolicy.get()) {
                        return mergeFinderRef.get().findMerges(mergeTrigger, segmentInfos, mergeContext);
                    } else {
                        return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
                    }
                }

                @Override
                public MergeSpecification findForcedMerges(
                    SegmentInfos segmentInfos,
                    int maxSegmentCount,
                    Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                    MergeContext mergeContext
                ) throws IOException {
                    if (useCustomMergePolicy.get()) {
                        // Disable force merges
                        return null;
                    } else {
                        return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
                    }
                }

                @Override
                public MergeSpecification findFullFlushMerges(
                    MergeTrigger mergeTrigger,
                    SegmentInfos segmentInfos,
                    MergeContext mergeContext
                ) throws IOException {
                    if (useCustomMergePolicy.get()) {
                        // Disable full flush merges
                        return null;
                    } else {
                        return super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
                    }
                }

                @Override
                public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
                    if (useCustomMergePolicy.get()) {
                        // Disable full flush merges
                        return null;
                    } else {
                        return super.findForcedDeletesMerges(segmentInfos, mergeContext);
                    }
                }
            };
        }

        static void setMergeFinder(MergeFinder mergeFinder) {
            mergeFinderRef.set(mergeFinder);
        }

        static void enableCustomMergePolicy() {
            useCustomMergePolicy.set(true);
        }

        static void disableCustomMergePolicy() {
            useCustomMergePolicy.set(false);
        }
    }

    interface MergeFinder {
        MergeFinder EMPTY = (mergeTrigger, segmentInfos, mergeContext) -> null;

        MergePolicy.MergeSpecification findMerges(
            MergeTrigger mergeTrigger,
            SegmentInfos segmentInfos,
            MergePolicy.MergeContext mergeContext
        );
    }

    public static class GenerationalFilesTrackingStatelessCommitService extends StatelessCommitService {

        public final Set<String> deletedGenerationalFiles = ConcurrentCollections.newConcurrentSet();

        public GenerationalFilesTrackingStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            super(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        public void onGenerationalFileDeletion(ShardId shardId, String filename) {
            deletedGenerationalFiles.add(filename);
            super.onGenerationalFileDeletion(shardId, filename);
        }
    }

    private static class GenerationalFilesTrackingIndexBlobStoreCacheDirectory extends IndexBlobStoreCacheDirectory {
        private final GenerationalFilesTracker generationalFilesTracker = new GenerationalFilesTracker();

        GenerationalFilesTrackingIndexBlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
            super(cacheService, shardId);
        }

        @Override
        protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
            var blobLocation = blobFileRanges.blobLocation();
            return generationalFilesTracker.trackIfIsGenerationalFile(super.doOpenInput(name, context, blobFileRanges), name, blobLocation);
        }

        @Override
        protected CacheBlobReader getCacheBlobReader(BlobLocation location) {
            // Use the direct executor so we can use the thread name in testBackgroundMergeCanRetainDeletedGenerationalFile
            return new MeteringCacheBlobReader(
                new ObjectStoreCacheBlobReader(
                    getBlobContainer(location.primaryTerm()),
                    location.blobName(),
                    getCacheService().getRangeSize(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                ) {
                    @Override
                    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                        ActionListener.run(listener, l -> {
                            InputStream stream = getRangeInputStream(position, length);
                            getCacheService().getShardReadThreadPoolExecutor().execute(() -> ActionListener.completeWith(l, () -> stream));
                        });
                    }
                },
                (int bytesRead, long timeToReadNanos) -> totalBytesReadFromObjectStore.add(bytesRead)
            );
        }

        public GenerationalFilesTracker getGenerationalFilesTracker() {
            return generationalFilesTracker;
        }
    }

    private static class GenerationalFilesTrackingSearchDirectory extends SearchDirectory {
        private final GenerationalFilesTracker generationalFilesTracker = new GenerationalFilesTracker();

        GenerationalFilesTrackingSearchDirectory(
            StatelessSharedBlobCacheService cacheService,
            CacheBlobReaderService cacheBlobReaderService,
            MutableObjectStoreUploadTracker objectStoreUploadTracker,
            ShardId shardId
        ) {
            super(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
        }

        @Override
        protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
            var blobLocation = blobFileRanges.blobLocation();
            return generationalFilesTracker.trackIfIsGenerationalFile(super.doOpenInput(name, context, blobFileRanges), name, blobLocation);
        }

        public GenerationalFilesTracker getGenerationalFilesTracker() {
            return generationalFilesTracker;
        }
    }

    private static final class GenerationalFilesTracker {
        record NameAndLocation(String name, BlobLocation blobLocation) {}

        private final Map<IndexInput, NameAndLocation> inputs = new HashMap<>();

        private synchronized IndexInput trackIfIsGenerationalFile(IndexInput input, String name, BlobLocation blobLocation) {
            if (StatelessCommitService.isGenerationalFile(name) == false) {
                return input;
            }
            var nameAndLocation = new NameAndLocation(name, blobLocation);
            if (inputs.putIfAbsent(input, nameAndLocation) != null) {
                fail("Input already tracked for " + name);
                return null;
            }
            return new FilterIndexInput("tracked(" + name + ')', input) {
                final AtomicBoolean closed = new AtomicBoolean();

                @Override
                public void close() throws IOException {
                    if (closed.compareAndSet(false, true)) {
                        try {
                            super.close();
                        } finally {
                            untrack(input, nameAndLocation);
                        }
                    }
                }

                @Override
                public IndexInput slice(String sliceDescription, long offset, long length) {
                    var searchInput = asInstanceOf(BlobCacheIndexInput.class, getDelegate());
                    // avoid BlobCacheBufferedIndexInput.trySliceBuffer which would read bytes from the heap instead of using the cache
                    return searchInput.doSlice(sliceDescription, offset, length);
                }
            };
        }

        private synchronized void untrack(IndexInput input, NameAndLocation nameAndLocation) {
            if (inputs.remove(input, nameAndLocation) == false) {
                fail("Input not tracked for " + nameAndLocation.name);
            }
        }

        private synchronized Map<String, Long> getGenerationalFilesAsMap() {
            return inputs.values()
                .stream()
                .collect(
                    Collectors.toUnmodifiableMap(NameAndLocation::name, e -> e.blobLocation().compoundFileGeneration(), (gen1, gen2) -> {
                        if (Objects.equals(gen1, gen2) == false) {
                            throw new AssertionError("Different generation " + gen1 + " vs " + gen2);
                        }
                        return gen1;
                    })
                );
        }

        static GenerationalFilesTracker getDirectoryGenerationalFilesTracker(final Directory directory) {
            Directory dir = directory;
            while (dir != null) {
                if (dir instanceof GenerationalFilesTrackingIndexBlobStoreCacheDirectory trackingIndexDirectory) {
                    return trackingIndexDirectory.getGenerationalFilesTracker();
                } else if (dir instanceof GenerationalFilesTrackingSearchDirectory trackingSearchDirectory) {
                    return trackingSearchDirectory.getGenerationalFilesTracker();
                } else if (dir instanceof FilterDirectory) {
                    dir = ((FilterDirectory) dir).getDelegate();
                } else {
                    dir = null;
                }
            }
            var e = new IllegalStateException(directory.getClass() + " is not a generational files tracking directory");
            assert false : e;
            throw e;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(GenerationalFilesTrackingStatelessPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ThreadPoolMergeScheduler.MERGE_THREAD_POOL_SCHEDULER.getKey(), true)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    @After
    public void disableCustomMergePolicy() {
        GenerationalFilesTrackingStatelessPlugin.disableCustomMergePolicy();
    }

    /**
     * Create an index with custom settings to prevent uncontrolled flushes
     */
    private String createIndex(int shards, int replicas) {
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(shards, replicas).put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "tiered")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .build()
        );
        return indexName;
    }

    public void testBackgroundMergeCanRetainDeletedGenerationalFile() throws Exception {
        GenerationalFilesTrackingStatelessPlugin.enableCustomMergePolicy();
        var nodeName = startMasterAndIndexNode(
            // Disable the cache, so we force all reads to fetch data from the blob store
            Settings.builder()
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1))
                // Ensure that commits are uploaded in the order that we want
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
                // Need at least 2 threads as we will block two
                .put(Stateless.MERGE_THREAD_POOL_SETTING + ".max", randomIntBetween(2, 4))
                .build()
        );

        final var indexName = createIndex(1, 0);
        var indexingShard = findIndexShard(resolveIndex(indexName), 0);
        var indexEngine = asInstanceOf(IndexEngine.class, indexingShard.getEngineOrNull());
        var blobStoreCacheDirectory = IndexBlobStoreCacheDirectory.unwrapDirectory(indexingShard.store().directory());

        // BCC4
        final long docsAfterSegment_0 = 10_000L;
        executeBulk(
            bulkRequest -> LongStream.range(0L, docsAfterSegment_0)
                .mapToObj(n -> client().prepareIndex(indexName).setId(String.valueOf(n)).setSource("segment", "_0"))
                .forEach(bulkRequest::add)
        );
        flush(indexName);

        assertThat(indexEngine.getCurrentGeneration(), equalTo(4L));
        assertBusyFilesLocations(
            blobStoreCacheDirectory,
            Map.ofEntries(
                // BCC3
                entry("segments_3", 3L),
                entry("segments_4", 4L),
                // BCC4 -> Contains documents with IDs [0, 10_000]
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L)
            )
        );

        // Flush two new segments that will create BCC5 and BCC6 in the blob store

        // BCC5
        executeBulk(bulkRequest -> bulkRequest.add(client().prepareIndex(indexName).setSource("segment", "_1")));
        flush(indexName);
        // BCC6
        executeBulk(bulkRequest -> bulkRequest.add(client().prepareIndex(indexName).setSource("segment", "_2")));
        flush(indexName);

        assertThat(indexEngine.getCurrentGeneration(), equalTo(6L));
        assertBusyFilesLocations(
            blobStoreCacheDirectory,
            Map.ofEntries(
                // BCC3
                entry("segments_3", 3L),
                // BCC4
                entry("segments_4", 4L),
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // BCC5
                entry("segments_5", 5L),
                entry("_1.cfe", 5L),
                entry("_1.cfs", 5L),
                entry("_1.si", 5L),
                // BCC6
                entry("segments_6", 6L),
                entry("_2.cfe", 6L),
                entry("_2.cfs", 6L),
                entry("_2.si", 6L)
            )
        );

        // The next BCC (with generation 7) will contain the generational file that will be retained by the background merge,
        // hence we'll inject a custom repository strategy that will block any reads by the Lucene Merge Thread #0 on that blob
        // to wait until the BCC containing that file is deleted wrongly.
        final var bccContainingFirstGenFile = BatchedCompoundCommit.blobNameFromGeneration(7);
        var blockBccContainingFirstGenFileReadByFirstMerge = new CountDownLatch(1);
        var bccContainingFirstGenFileReadFirstMergeBlocked = new CountDownLatch(1);
        var blockBccContainingFirstGenFileReadBySecondMerge = new CountDownLatch(1);
        var bccContainingFirstGenFileReadSecondMergeBlocked = new CountDownLatch(1);
        var bccContainingFirstGenFileDeleted = new CountDownLatch(1);
        PlainActionFuture<IOException> fileNotFoundExceptionFuture = new PlainActionFuture<>();
        setNodeRepositoryStrategy(nodeName, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                maybeBlock(blobName);
                try {
                    return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
                } catch (IOException e) {
                    fileNotFoundExceptionFuture.onResponse(e);
                    throw e;
                }
            }

            void maybeBlock(String blobName) {
                if (blobName.equals(bccContainingFirstGenFile)) {
                    var currentThreadName = Thread.currentThread().getName();
                    if (currentThreadName.contains("stateless.merge][T#1")) {
                        blockRead(bccContainingFirstGenFileReadFirstMergeBlocked, blockBccContainingFirstGenFileReadByFirstMerge);
                    } else if (currentThreadName.contains("stateless.merge][T#2")) {
                        blockRead(bccContainingFirstGenFileReadSecondMergeBlocked, blockBccContainingFirstGenFileReadBySecondMerge);
                    }
                }
            }

            private void blockRead(CountDownLatch signalReadBlocked, CountDownLatch unblockReadLatch) {
                logger.info("--> Blocking reads from {}", Thread.currentThread().getName());
                signalReadBlocked.countDown();
                try {
                    // 10 seconds might be not enough since the file deletion takes a bit to be executed
                    assertTrue(unblockReadLatch.await(60, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }

            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                List<String> blobsToDelete = new ArrayList<>();
                blobNames.forEachRemaining(blobsToDelete::add);
                var shouldTrigger = blobsToDelete.stream().anyMatch(name -> name.contains(bccContainingFirstGenFile));
                originalRunnable.run();
                if (shouldTrigger) {
                    bccContainingFirstGenFileDeleted.countDown();
                }
            }
        });

        // Before we create a new segment and the first generational file attached to segment _0, set a new merge finder that
        // will trigger a background merge for segments _0, _1, _2 after the new segment (_3) is flushed and segment _0 has
        // a generational file with generation 1 attached to it. That should trigger a read against the blob store while the
        // three segments are merged.
        var firstMergeTriggered = new AtomicBoolean(false);
        GenerationalFilesTrackingStatelessPlugin.setMergeFinder((mergeTrigger, segmentInfos, mergeContext) -> {
            if (firstMergeTriggered.compareAndSet(false, true)) {
                var mergeSpecification = new MergePolicy.MergeSpecification();
                var toMerge = StreamSupport.stream(segmentInfos.spliterator(), false)
                    .filter(
                        segmentCommitInfo -> segmentCommitInfo.info.name.equals("_0")
                            || segmentCommitInfo.info.name.equals("_1")
                            || segmentCommitInfo.info.name.equals("_2")
                    )
                    .toList();

                var segmentWithDeletions = StreamSupport.stream(segmentInfos.spliterator(), false)
                    .filter(f -> f.info.name.equals("_0"))
                    .findFirst()
                    .orElseThrow();
                // Ensure that the segment _0 has a generational file attached to it
                assertThat(segmentWithDeletions.getDocValuesGen(), is(equalTo(1L)));

                assertThat(toMerge.size(), is(equalTo(3)));

                mergeSpecification.add(new MergePolicy.OneMerge(toMerge));
                return mergeSpecification;
            } else {
                return null;
            }
        });

        // Delete a document present in segment _0, it should create a new generational file in generation 1
        executeBulk(
            bulkRequest -> bulkRequest.add(client().prepareDelete(indexName, String.valueOf(1)))
                .add(client().prepareIndex(indexName).setSource("segment", "_3"))
        );
        flush(indexName);

        assertThat(indexEngine.getCurrentGeneration(), equalTo(8L));
        assertBusyFilesLocations(
            blobStoreCacheDirectory,
            Map.ofEntries(
                // BCC3
                entry("segments_3", 3L),
                // BCC4
                entry("segments_4", 4L),
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // BCC5
                entry("segments_5", 5L),
                entry("_1.cfe", 5L),
                entry("_1.cfs", 5L),
                entry("_1.si", 5L),
                // BCC6
                entry("segments_6", 6L),
                entry("_2.cfe", 6L),
                entry("_2.cfs", 6L),
                entry("_2.si", 6L),
                // BCC7
                // Deletes might cause a refresh when InternalEngine.getVersionFromMap is called,
                // that will generate an empty commit (segments_7)
                entry("segments_7", 7L),
                entry("segments_8", 7L),
                entry("_3.cfe", 7L),
                entry("_3.cfs", 7L),
                entry("_3.si", 7L),
                entry("_0_1_Lucene90_0.dvd", 7L),
                entry("_0_1_Lucene90_0.dvm", 7L),
                entry("_0_1.fnm", 7L)
            )
        );

        // Wait until the background merge tries to read _0_1_Lucene90_0.dvd from BCC7
        safeAwait(bccContainingFirstGenFileReadFirstMergeBlocked);

        executeBulk(bulkRequest -> bulkRequest.add(client().prepareIndex(indexName).setSource("segment", "_5")));
        flush(indexName);

        // The commit with generation 8 was reserved for the background merge, hence the jump to generation 9
        assertThat(indexEngine.getCurrentGeneration(), equalTo(9L));
        assertBusyFilesLocations(
            blobStoreCacheDirectory,
            Map.ofEntries(
                // BCC3
                entry("segments_3", 3L),
                // BCC4
                entry("segments_4", 4L),
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // BCC5
                entry("segments_5", 5L),
                entry("_1.cfe", 5L),
                entry("_1.cfs", 5L),
                entry("_1.si", 5L),
                // BCC6
                entry("segments_6", 6L),
                entry("_2.cfe", 6L),
                entry("_2.cfs", 6L),
                entry("_2.si", 6L),
                // BCC7
                entry("segments_7", 7L),
                entry("segments_8", 7L),
                entry("_3.cfe", 7L),
                entry("_3.cfs", 7L),
                entry("_3.si", 7L),
                // The generational doc value files are carried over but their BlobLocations do not after ES-8897
                entry("_0_1_Lucene90_0.dvd", 7L),
                entry("_0_1_Lucene90_0.dvm", 7L),
                entry("_0_1.fnm", 7L),
                // BCC9 (segment _4 is reserved by the previous background merge)
                entry("segments_9", 9L),
                entry("_5.cfe", 9L),
                entry("_5.cfs", 9L),
                entry("_5.si", 9L)
            )
        );

        // We want BCC7 (which contains _0_1_Lucene90_0.dvd and segment _3, and it's read by the background Lucene Merge Thread #0)
        // to be merged away with the next segment (_5) so the BCC7 is not referenced anymore and can be deleted
        // (until the gen files bug is solved).
        var lastMergeTriggered = new AtomicBoolean(false);
        GenerationalFilesTrackingStatelessPlugin.setMergeFinder((mergeTrigger, segmentInfos, mergeContext) -> {
            if (lastMergeTriggered.compareAndSet(false, true)) {
                var toMerge = StreamSupport.stream(segmentInfos.spliterator(), false)
                    .filter(segmentCommitInfo -> segmentCommitInfo.info.name.equals("_3") || segmentCommitInfo.info.name.equals("_5"))
                    .toList();
                assertThat(toMerge.size(), equalTo(2));

                var mergeSpecification = new MergePolicy.MergeSpecification();
                mergeSpecification.add(new MergePolicy.OneMerge(toMerge));
                return mergeSpecification;
            } else {
                return null;
            }
        });

        // Execute a bulk that will trigger a background merge and includes a new deletion for segment _0, that will roll over
        // the generational file generation (from 1 -> 2), making _0_1_Lucene90_0.dvd not needed anymore and not referenced after the
        // triggered merge finishes.
        executeBulk(
            bulkRequest -> bulkRequest.add(client().prepareDelete(indexName, String.valueOf(2)))
                .add(client().prepareIndex(indexName).setSource("segment", "_6"))
        );
        flush(indexName);

        // Wait until the second background merge tries to read BCC7
        safeAwait(bccContainingFirstGenFileReadSecondMergeBlocked);

        assertThat(indexEngine.getCurrentGeneration(), equalTo(10L));
        assertBusyFilesLocations(
            blobStoreCacheDirectory,
            Map.ofEntries(
                // BCC3
                entry("segments_3", 3L),
                // BCC4
                entry("segments_4", 4L),
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // BCC5
                entry("segments_5", 5L),
                entry("_1.cfe", 5L),
                entry("_1.cfs", 5L),
                entry("_1.si", 5L),
                // BCC6
                entry("segments_6", 6L),
                entry("_2.cfe", 6L),
                entry("_2.cfs", 6L),
                entry("_2.si", 6L),
                // BCC7
                entry("segments_7", 7L),
                entry("segments_8", 7L),
                entry("_3.cfe", 7L),
                entry("_3.cfs", 7L),
                entry("_3.si", 7L),
                // The generational doc value files are carried over but their BlobLocations do not after ES-8897
                entry("_0_1_Lucene90_0.dvd", 7L),
                entry("_0_1_Lucene90_0.dvm", 7L),
                entry("_0_1.fnm", 7L),
                // BCC9 (segment _4 is reserved by the previous background merge)
                entry("segments_9", 9L),
                entry("_5.cfe", 9L),
                entry("_5.cfs", 9L),
                entry("_5.si", 9L),
                // BCC10 (It includes the new generational file with new deletions)
                entry("segments_a", 10L),
                entry("_6.cfe", 10L),
                entry("_6.cfs", 10L),
                entry("_6.si", 10L),
                entry("_0_2_Lucene90_0.dvd", 10L),
                entry("_0_2_Lucene90_0.dvm", 10L),
                entry("_0_2.fnm", 10L)
            )
        );

        // Unblock the second background merge
        blockBccContainingFirstGenFileReadBySecondMerge.countDown();

        // Wait until the background merge (merging _3 and _5) finishes
        assertBusy(() -> {
            var runningMerges = client().admin()
                .indices()
                .prepareStats(indexName)
                .setMerge(true)
                .get()
                .getIndex(indexName)
                .getPrimaries()
                .getMerge()
                .getCurrent();
            assertThat(runningMerges, is(equalTo(1L)));
        });
        // Ensure that the background merge is flushed and uploaded into the blob store
        flush(indexName);
        // Once generational files tracking is enabled, the blob shouldn't be deleted until the on-going merge finishes
        // we check that by ensuring that the BlobLocation for these files are still available.
        var commitService = internalCluster().getInstance(StatelessCommitService.class, nodeName);
        var genFilesReadByMergeThread = Set.of("_0_1.fnm", "_0_1_Lucene90_0.dvd", "_0_1_Lucene90_0.dvm");
        for (String genFileReadyByMergeThread : genFilesReadByMergeThread) {
            var blobLocation = commitService.getBlobLocation(new ShardId(resolveIndex(indexName), 0), genFileReadyByMergeThread);
            assertThat(blobLocation, notNullValue());
        }
        // Let the read continue
        blockBccContainingFirstGenFileReadByFirstMerge.countDown();
        expectThrows(TimeoutException.class, () -> fileNotFoundExceptionFuture.get(500, TimeUnit.MILLISECONDS));
        // Speed up the file deletion consistency check (it's shared with the translog consistency checks)
        indexDocs(indexName, 1);
        // Eventually, the blob will be deleted
        safeAwait(bccContainingFirstGenFileDeleted);

        // Now the merged generational files are gone
        for (String genFileReadyByMergeThread : genFilesReadByMergeThread) {
            var blobLocation = commitService.getBlobLocation(new ShardId(resolveIndex(indexName), 0), genFileReadyByMergeThread);
            assertThat(blobLocation, nullValue());
        }
    }

    public void testSearchShardGenerationFilesRetention() throws Exception {
        var masterNode = startMasterOnlyNode();
        var indexNode = startIndexNode();

        final var indexName = createIndex(1, 0);
        var indexingShard = findIndexShard(resolveIndex(indexName), 0);
        var indexEngine = asInstanceOf(IndexEngine.class, indexingShard.getEngineOrNull());
        var indexDirectory = IndexDirectory.unwrapDirectory(indexingShard.store().directory());

        // create docs in segment core _0
        final long docsAfterSegment_0 = 1_000L;
        executeBulk(
            bulkRequest -> LongStream.range(0L, docsAfterSegment_0)
                .mapToObj(n -> client(masterNode).prepareIndex(indexName).setId(String.valueOf(n)).setSource("segment", "_0"))
                .forEach(bulkRequest::add)
        );
        flush(indexName);

        assertThat(indexEngine.getCurrentGeneration(), equalTo(4L));
        assertBusyFilesLocations(
            indexDirectory,
            Map.ofEntries(
                entry("segments_4", 4L),
                // new segment core _0
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L)
            )
        );

        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of());
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_0));
        assertThat(indexingShard.docStats().getDeleted(), equalTo(0L));

        // create 10 additional docs in segment core _1
        long docsAfterSegment_1 = docsAfterSegment_0 + 10L;
        executeBulk(
            bulkRequest -> LongStream.range(docsAfterSegment_0, docsAfterSegment_1)
                .mapToObj(n -> client(masterNode).prepareIndex(indexName).setId(String.valueOf(n)).setSource("segment", "_1"))
                .forEach(bulkRequest::add)
        );
        flush(indexName);

        assertThat(indexEngine.getCurrentGeneration(), equalTo(5L));
        assertBusyFilesLocations(
            indexDirectory,
            Map.ofEntries(
                entry("segments_5", 5L),
                // referenced segment core _0
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // new segment core _1
                entry("_1.cfe", 5L),
                entry("_1.cfs", 5L),
                entry("_1.si", 5L)
            )
        );

        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of());
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_1));
        assertThat(indexingShard.docStats().getDeleted(), equalTo(0L));

        // delete every 10th doc from segment core _0 and produce new segment core _2
        executeBulk(
            bulkRequest -> LongStream.range(0L, docsAfterSegment_0)
                .filter(n -> n % 10 == 0)
                .mapToObj(n -> client(masterNode).prepareDelete(indexName, String.valueOf(n)))
                .forEach(bulkRequest::add)
        );
        flush(indexName);

        var filesLocations = Map.ofEntries(
            entry("segments_6", 6L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // referenced segment core _1
            entry("_1.cfe", 5L),
            entry("_1.cfs", 5L),
            entry("_1.si", 5L),
            // new segment core _2
            entry("_2.cfe", 6L),
            entry("_2.cfs", 6L),
            entry("_2.si", 6L),
            // new generational files for _0
            entry("_0_1.fnm", 6L),
            entry("_0_1_Lucene90_0.dvd", 6L),
            entry("_0_1_Lucene90_0.dvm", 6L)
        );

        long deletedDocs = docsAfterSegment_0 / 10L;
        long docsAfterSegment_2 = docsAfterSegment_1 - deletedDocs;
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_2));

        assertThat(indexEngine.getCurrentGeneration(), equalTo(6L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        // .dvm and .fnm are also opened but then fully read once and closed.
        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of("_0_1_Lucene90_0.dvd", 6L));

        // batched compound commit for generation 6 exists in the object store (stateless_commit_6)
        final String bccBlobName = StatelessCompoundCommit.blobNameFromGeneration(6L);
        var blobContainer = indexDirectory.getBlobStoreCacheDirectory().getBlobContainer(indexingShard.getOperationPrimaryTerm());
        assertBusy(() -> assertThat(blobContainer.listBlobs(OperationPurpose.INDICES).keySet(), hasItem(bccBlobName)));

        // now start a search shard on commit generation 6
        startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        final var searchShard = findSearchShard(resolveIndex(indexName), 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());

        assertBusyRefreshedGeneration(searchEngine, equalTo(6L));
        assertBusyFilesLocations(searchDirectory, filesLocations);
        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(new PrimaryTermAndGeneration(1L, 4L), new PrimaryTermAndGeneration(1L, 5L), new PrimaryTermAndGeneration(1L, 6L))
        );

        // create 10 additional docs in segment core _3
        long docsAfterSegment_3 = docsAfterSegment_2 + 10L;
        executeBulk(
            bulkRequest -> LongStream.range(docsAfterSegment_1, docsAfterSegment_1 + 10L)
                .mapToObj(n -> client(masterNode).prepareIndex(indexName).setId(String.valueOf(n)).setSource("segment", "_3"))
                .forEach(bulkRequest::add)
        );
        flush(indexName);

        filesLocations = Map.ofEntries(
            entry("segments_7", 7L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // referenced segment core _1
            entry("_1.cfe", 5L),
            entry("_1.cfs", 5L),
            entry("_1.si", 5L),
            // referenced segment core _2
            entry("_2.cfe", 6L),
            entry("_2.cfs", 6L),
            entry("_2.si", 6L),
            // new segment core _3
            entry("_3.cfe", 7L),
            entry("_3.cfs", 7L),
            entry("_3.si", 7L),
            entry("_0_1.fnm", 6L),
            entry("_0_1_Lucene90_0.dvd", 6L),
            entry("_0_1_Lucene90_0.dvm", 6L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(7L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        assertBusyRefreshedGeneration(searchEngine, equalTo(7L));
        assertBusyFilesLocations(
            searchDirectory,
            Map.ofEntries(
                entry("segments_7", 7L),
                // referenced segment core _0
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // referenced segment core _1
                entry("_1.cfe", 5L),
                entry("_1.cfs", 5L),
                entry("_1.si", 5L),
                // referenced segment core _2
                entry("_2.cfe", 6L),
                entry("_2.cfs", 6L),
                entry("_2.si", 6L),
                // new segment core _3
                entry("_3.cfe", 7L),
                entry("_3.cfs", 7L),
                entry("_3.si", 7L),
                // blob locations for generational files of _0 are not updated
                entry("_0_1.fnm", 6L),
                entry("_0_1_Lucene90_0.dvd", 6L),
                entry("_0_1_Lucene90_0.dvm", 6L)
            )
        );
        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of("_0_1_Lucene90_0.dvd", 6L));

        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(
                new PrimaryTermAndGeneration(1L, 4L),
                new PrimaryTermAndGeneration(1L, 5L),
                new PrimaryTermAndGeneration(1L, 6L),
                new PrimaryTermAndGeneration(1L, 7L)
            )
        );

        // now force-merge to 2 segments: segments _1, _2 and _3 should be merged together as they are smaller than _0, producing _4
        var forceMerge = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(2).setFlush(false).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(2));
        flush(indexName);

        filesLocations = Map.ofEntries(
            entry("segments_8", 8L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // new segment core _4
            entry("_4.cfe", 8L),
            entry("_4.cfs", 8L),
            entry("_4.si", 8L),
            entry("_0_1.fnm", 6L),
            entry("_0_1_Lucene90_0.dvd", 6L),
            entry("_0_1_Lucene90_0.dvm", 6L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(8L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        assertBusyRefreshedGeneration(searchEngine, equalTo(8L));
        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of("_0_1_Lucene90_0.dvd", 6L));

        // When refreshing the Lucene index, Lucene only opens new segments and carries over the existing segment reader associated with
        // already opened segments (see SegmentReader and SegmentCoreReaders). It means that the generational docs values file
        // _0_1_Lucene90_0.dvd was opened on commit generation 6 on the search shard (and therefore use the stateless_commit_6 blob), and
        // its IndexInput is still opened and carried over when refreshing the Lucene index on commit generation 8.
        assertBusy(
            () -> assertThat(
                searchEngine.getAcquiredPrimaryTermAndGenerations(),
                contains(new PrimaryTermAndGeneration(1L, 4L), new PrimaryTermAndGeneration(1L, 6L), new PrimaryTermAndGeneration(1L, 8L))
            )
        );
        assertBusyFilesLocations(
            searchDirectory,
            Map.ofEntries(
                entry("segments_8", 8L),
                // referenced segment core _0
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // new segment core _4
                entry("_4.cfe", 8L),
                entry("_4.cfs", 8L),
                entry("_4.si", 8L),
                // blob locations for generational files of _0 are not updated
                entry("_0_1.fnm", 6L),
                entry("_0_1_Lucene90_0.dvd", 6L),
                entry("_0_1_Lucene90_0.dvm", 6L)
            )
        );

        // check that blob 'stateless_commit_6' exists in the object store
        var blobContainerBefore = indexDirectory.getBlobStoreCacheDirectory().getBlobContainer(indexingShard.getOperationPrimaryTerm());
        assertBusy(() -> assertThat(blobContainerBefore.listBlobs(OperationPurpose.INDICES).keySet(), hasItem(bccBlobName)));

        // relocate the indexing shard
        var newIndexNode = startIndexNode();
        ensureStableCluster(4);
        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNode), indexName);
        ensureGreen(indexName);
        internalCluster().stopNode(indexNode);

        indexingShard = findIndexShard(resolveIndex(indexName), 0, newIndexNode);
        indexEngine = asInstanceOf(IndexEngine.class, indexingShard.getEngineOrNull());
        indexDirectory = IndexDirectory.unwrapDirectory(indexingShard.store().directory());

        filesLocations = Map.ofEntries(
            entry("segments_9", 9L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // referenced segment core _4
            entry("_4.cfe", 8L),
            entry("_4.cfs", 8L),
            entry("_4.si", 8L),
            entry("_0_1.fnm", 8L),
            entry("_0_1_Lucene90_0.dvd", 8L),
            entry("_0_1_Lucene90_0.dvm", 8L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(9L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        // the new indexing shard opens the generational files on the generation 8L (flush before relocation)
        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of("_0_1_Lucene90_0.dvd", 8L));
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        var blobContainerAfter = indexDirectory.getBlobStoreCacheDirectory().getBlobContainer(indexingShard.getOperationPrimaryTerm());
        // commit generation 6 is still used by the search shard, it should exist in the object store
        assertThat(blobContainerAfter.listBlobs(OperationPurpose.INDICES).keySet(), hasItem(bccBlobName));

        assertBusyRefreshedGeneration(searchEngine, equalTo(9L));

        // search shard still uses generation 6
        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(
                new PrimaryTermAndGeneration(1L, 4L),
                new PrimaryTermAndGeneration(1L, 6L),
                new PrimaryTermAndGeneration(1L, 8L),
                new PrimaryTermAndGeneration(1L, 9L)
            )
        );
        assertBusyFilesLocations(
            searchDirectory,
            Map.ofEntries(
                entry("segments_9", 9L),
                // referenced segment core _0
                entry("_0.cfe", 4L),
                entry("_0.cfs", 4L),
                entry("_0.si", 4L),
                // referenced segment core _4
                entry("_4.cfe", 8L),
                entry("_4.cfs", 8L),
                entry("_4.si", 8L),
                // blob locations for generational files of _0 are not updated
                entry("_0_1.fnm", 6L),
                entry("_0_1_Lucene90_0.dvd", 6L),
                entry("_0_1_Lucene90_0.dvm", 6L)
            )
        );

        // clear the cache on the search shard to force reading data from object store again
        var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);
        searchCacheService.forceEvict(fileCacheKey -> bccBlobName.equals(fileCacheKey.fileName()));

        // With ES-7496 resolved we can fully read the generational file without triggering a NoSuchFileException
        try (var searcher = searchShard.acquireSearcher("test")) {
            assertThat(searcher.getDirectoryReader().getIndexCommit().getGeneration(), equalTo(9L));
            readGenerationalDocValues(searcher);
        }

        // force-merge to one segment to get rid of all generational files
        forceMerge = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(2));

        filesLocations = Map.ofEntries(
            entry("segments_a", 10L),
            // referenced segment core _5
            entry("_5.cfe", 10L),
            entry("_5.cfs", 10L),
            entry("_5.si", 10L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(10L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        assertBusyOpenedGenerationalFiles(indexDirectory.getBlobStoreCacheDirectory(), Map.of());
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of());
        assertBusyRefreshedGeneration(searchEngine, equalTo(10L));
        assertBusy(() -> assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(new PrimaryTermAndGeneration(1L, 10L))));
        assertBusyFilesLocations(searchDirectory, filesLocations);

        assertHitCount(client().prepareSearch(indexName).setSize(0), docsAfterSegment_3);
    }

    public void testOnGenerationalFileDeletion() throws Exception {
        var indexNode = startMasterAndIndexNode();
        final var indexName = createIndex(1, 0);

        var indexingShard = findIndexShard(resolveIndex(indexName), 0);
        var indexEngine = asInstanceOf(IndexEngine.class, indexingShard.getEngineOrNull());
        final Set<String> deletedGenerationalFiles = ((GenerationalFilesTrackingStatelessCommitService) indexEngine
            .getStatelessCommitService()).deletedGenerationalFiles;

        executeBulk(
            bulkRequest -> LongStream.range(0L, 100)
                .mapToObj(n -> client(indexNode).prepareIndex(indexName).setId(String.valueOf(n)).setSource("segment", "_0"))
                .forEach(bulkRequest::add)
        );
        flush(indexName);

        // Create some generational files
        executeBulk(bulkRequest -> bulkRequest.add(client(indexNode).prepareDelete(indexName, "42")));
        flush(indexName);

        assertThat(deletedGenerationalFiles, empty());

        // merge away the generational files
        forceMerge();
        assertBusy(() -> assertThat(deletedGenerationalFiles, equalTo(Set.of("_0_1.fnm", "_0_1_Lucene90_0.dvd", "_0_1_Lucene90_0.dvm"))));
    }

    private static void executeBulk(Consumer<BulkRequestBuilder> consumer) {
        var bulkRequest = client().prepareBulk();
        consumer.accept(bulkRequest);
        assertNoFailures(bulkRequest.get());
    }

    private static void assertBusyFilesLocations(Directory directory, Map<String, Long> expectedLocations) throws Exception {
        var blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(directory);
        assertBusy(() -> {
            for (var expected : expectedLocations.entrySet()) {
                var fileName = expected.getKey();
                var blobLocation = BlobStoreCacheDirectoryTestUtils.getBlobLocation(blobStoreCacheDirectory, fileName);
                var actualCompoundCommitGeneration = blobLocation != null ? blobLocation.compoundFileGeneration() : null;
                assertThat(
                    "BlobLocation of file ["
                        + fileName
                        + "] does not match expected compound commit generation ["
                        + expected.getValue()
                        + ']',
                    actualCompoundCommitGeneration,
                    equalTo(expected.getValue())
                );
            }
            assertDirectoryContents(directory, expectedLocations);
        });
    }

    private static void assertDirectoryContents(Directory directory, Map<String, Long> expectedLocations) throws IOException {
        assertThat(List.of(directory.listAll()), equalTo(expectedLocations.keySet().stream().sorted(String::compareTo).toList()));
    }

    private static void assertBusyRefreshedGeneration(SearchEngine searchEngine, Matcher<Long> matcher) throws Exception {
        assertBusy(() -> {
            // Assert SearchEngine#currentPrimaryTermGeneration first, since it is updated after the SearchEngine's reader has been
            // refreshed with the latest commit notification. This way we ensure that the following acquireSearcher() won't acquire a
            // searcher in the middle of the refresh, which would retain files of the previous commit in the SearchDirectory and makes
            // any future assertDirectoryContents to fail due to retained files from a previous commit.
            var searchEngineTermAndGen = SearchEngineTestUtils.getCurrentPrimaryTermAndGeneration(searchEngine);
            assertThat(searchEngineTermAndGen.generation(), matcher);

            try (var searcher = searchEngine.acquireSearcher("search_engine_directory_reader_generation")) {
                assertThat(searcher.getDirectoryReader().getIndexCommit().getGeneration(), matcher);
            }
        });
    }

    private static void assertBusyOpenedGenerationalFiles(Directory directory, Map<String, Long> generationalFiles) throws Exception {
        var generationalFilesTracker = GenerationalFilesTracker.getDirectoryGenerationalFilesTracker(directory);
        assertThat(generationalFilesTracker, notNullValue());
        assertBusy(() -> assertThat(generationalFilesTracker.getGenerationalFilesAsMap(), equalTo(generationalFiles)));
    }

    /**
     * Makes sure to read the generational files (doc by doc) and returns the total number of live docs, without closing the Searcher.
     */
    private static long readGenerationalDocValues(Engine.Searcher searcher) throws IOException {
        long totalLiveDocs = 0L;
        for (var leaf : searcher.getDirectoryReader().leaves()) {
            var leafReader = leaf.reader();
            var unwrapped = FilterLeafReader.unwrap(leafReader);
            totalLiveDocs += unwrapped.maxDoc() - unwrapped.numDeletedDocs();

            if (unwrapped instanceof CodecReader codecReader) {
                for (var fieldInfo : codecReader.getFieldInfos()) {
                    if (fieldInfo.isSoftDeletesField()) {
                        NumericDocValues numericDocValues = codecReader.getNumericDocValues(fieldInfo.getName());
                        int docID;
                        while ((docID = numericDocValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                            assertEquals(numericDocValues.docID(), docID);
                            numericDocValues.longValue();
                        }
                    }
                }
            }
        }
        return totalLiveDocs;
    }
}
