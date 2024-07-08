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
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.Map.entry;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class GenerationalDocValuesIT extends AbstractStatelessIntegTestCase {

    /**
     * Plugin to track Lucene opened generational files.
     */
    public static class GenerationalFilesTrackingStatelessPlugin extends Stateless {

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
        protected SearchDirectory createSearchDirectory(
            StatelessSharedBlobCacheService cacheService,
            CacheBlobReaderService cacheBlobReaderService,
            MutableObjectStoreUploadTracker tracker,
            ShardId shardId
        ) {
            return new GenerationalFilesTrackingSearchDirectory(cacheService, cacheBlobReaderService, tracker, shardId);
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService
        ) {
            return new GenerationalFilesTrackingStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                client,
                commitCleaner,
                cacheWarmingService
            );
        }
    }

    public static class GenerationalFilesTrackingStatelessCommitService extends StatelessCommitService {

        public final Set<String> deletedGenerationalFiles = ConcurrentCollections.newConcurrentSet();

        public GenerationalFilesTrackingStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService
        ) {
            super(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService);
        }

        @Override
        public void onGenerationalFileDeletion(ShardId shardId, String filename) {
            deletedGenerationalFiles.add(filename);
            super.onGenerationalFileDeletion(shardId, filename);
        }
    }

    private static class GenerationalFilesTrackingSearchDirectory extends SearchDirectory {

        record NameAndLocation(String name, BlobLocation blobLocation) {}

        private final Map<IndexInput, NameAndLocation> inputs = new HashMap<>();

        GenerationalFilesTrackingSearchDirectory(
            StatelessSharedBlobCacheService cacheService,
            CacheBlobReaderService cacheBlobReaderService,
            MutableObjectStoreUploadTracker tracker,
            ShardId shardId
        ) {
            super(cacheService, cacheBlobReaderService, tracker, shardId);
        }

        @Override
        protected IndexInput doOpenInput(String name, IOContext context, BlobLocation blobLocation) {
            var input = super.doOpenInput(name, context, blobLocation);
            return StatelessCommitService.isGenerationalFile(name) ? track(input, name, blobLocation) : input;
        }

        private synchronized IndexInput track(IndexInput input, String name, BlobLocation blobLocation) {
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
                    var searchInput = asInstanceOf(SearchIndexInput.class, getDelegate());
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

        static GenerationalFilesTrackingSearchDirectory unwrapTrackingDirectory(final Directory directory) {
            Directory dir = directory;
            while (dir != null) {
                if (dir instanceof GenerationalFilesTrackingSearchDirectory trackingSearchDirectory) {
                    return trackingSearchDirectory;
                } else if (dir instanceof IndexDirectory indexDirectory) {
                    dir = indexDirectory.getSearchDirectory();
                } else if (dir instanceof FilterDirectory) {
                    dir = ((FilterDirectory) dir).getDelegate();
                } else {
                    dir = null;
                }
            }
            var e = new IllegalStateException(
                directory.getClass() + " cannot be unwrapped as " + GenerationalFilesTrackingSearchDirectory.class
            );
            assert false : e;
            throw e;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(GenerationalFilesTrackingStatelessPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L));
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

        assertBusyOpenedGenerationalFiles(indexDirectory.getSearchDirectory(), Map.of());
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

        assertBusyOpenedGenerationalFiles(indexDirectory.getSearchDirectory(), Map.of());
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
        assertBusyOpenedGenerationalFiles(indexDirectory.getSearchDirectory(), Map.of("_0_1_Lucene90_0.dvd", 6L));

        // batched compound commit for generation 6 exists in the object store (stateless_commit_6)
        final String bccBlobName = StatelessCompoundCommit.blobNameFromGeneration(6L);
        var blobContainer = indexDirectory.getSearchDirectory().getBlobContainer(indexingShard.getOperationPrimaryTerm());
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
            // copied generational files for _0
            entry("_0_1.fnm", 7L),
            entry("_0_1_Lucene90_0.dvd", 7L),
            entry("_0_1_Lucene90_0.dvm", 7L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(7L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        assertBusyOpenedGenerationalFiles(indexDirectory.getSearchDirectory(), Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        assertBusyRefreshedGeneration(searchEngine, equalTo(7L));
        assertBusyFilesLocations(searchDirectory, filesLocations);
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
            // copied generational files for _0
            entry("_0_1.fnm", 8L),
            entry("_0_1_Lucene90_0.dvd", 8L),
            entry("_0_1_Lucene90_0.dvm", 8L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(8L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        assertBusyOpenedGenerationalFiles(indexDirectory.getSearchDirectory(), Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        assertBusyRefreshedGeneration(searchEngine, equalTo(8L));
        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of("_0_1_Lucene90_0.dvd", 6L));

        // When refreshing the Lucene index, Lucene only opens new segments and carries over the existing segment reader associated with
        // already opened segments (see SegmentReader and SegmentCoreReaders). It means that the generational docs values file
        // _0_1_Lucene90_0.dvd was opened on commit generation 6 on the search shard (and therefore use the stateless_commit_6 blob), and
        // its IndexInput is still opened and carried over when refreshing the Lucene index on commit generation 8.
        //
        // Since the search shard computes the BCC dependencies on the StatelessCompoundCommit alone, it only reports commit generations
        // 4 and 8 as used, not 6. This is OK for now because the indexing shard retains the corresponding blob for its own usage, but if
        // that changes then the search shard is left with a SearchIndexInput that uses a blob that can be pruned at any time.
        assertBusy(
            () -> assertThat(
                searchEngine.getAcquiredPrimaryTermAndGenerations(),
                contains(new PrimaryTermAndGeneration(1L, 4L), new PrimaryTermAndGeneration(1L, 8L))
            )
        );

        // check that blob 'stateless_commit_6' exists in the object store
        var blobContainerBefore = indexDirectory.getSearchDirectory().getBlobContainer(indexingShard.getOperationPrimaryTerm());
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
            // copied generational files for _0
            entry("_0_1.fnm", 9L),
            entry("_0_1_Lucene90_0.dvd", 9L),
            entry("_0_1_Lucene90_0.dvm", 9L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(9L));
        assertBusyFilesLocations(indexDirectory, filesLocations);

        // the new indexing shard opens the generational files on the generation 8L (flush before relocation)
        assertBusyOpenedGenerationalFiles(indexDirectory.getSearchDirectory(), Map.of("_0_1_Lucene90_0.dvd", 8L));
        assertThat(indexingShard.docStats().getCount(), equalTo(docsAfterSegment_3));

        // after some time the new indexing shard can delete unused commits, including generation 6
        var blobContainerAfter = indexDirectory.getSearchDirectory().getBlobContainer(indexingShard.getOperationPrimaryTerm());
        assertBusy(() -> assertThat(blobContainerAfter.listBlobs(OperationPurpose.INDICES).keySet(), not(hasItem(bccBlobName))));

        assertBusyRefreshedGeneration(searchEngine, equalTo(9L));
        assertBusyFilesLocations(searchDirectory, filesLocations);

        // search shard still uses generation 6 which has been deleted from object store
        assertBusyOpenedGenerationalFiles(searchDirectory, Map.of("_0_1_Lucene90_0.dvd", 6L));
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(new PrimaryTermAndGeneration(1L, 4L), new PrimaryTermAndGeneration(1L, 8L), new PrimaryTermAndGeneration(1L, 9L))
        );

        // clear the cache on the search shard to force reading data from object store again
        var searchCacheService = SearchDirectoryTestUtils.getCacheService(searchDirectory);
        searchCacheService.forceEvict(fileCacheKey -> bccBlobName.equals(fileCacheKey.fileName()));

        // Issue ES-7496:
        // Generational file term/generation used on search shards are not retained on indexing shards, any read on a gen. file will
        // trigger a NoSuchFileException.
        Exception e = expectThrows(Exception.class, () -> {
            try (var searcher = searchShard.acquireSearcher("test")) {
                assertThat(searcher.getDirectoryReader().getIndexCommit().getGeneration(), equalTo(9L));
                // make sure to read the generation files to trigger a cache miss
                readGenerationalDocValues(searcher);
            }
        });
        var cause = ExceptionsHelper.unwrap(e, NoSuchFileException.class);
        assertThat(cause, notNullValue());
        assertThat(cause.getMessage(), containsString(bccBlobName));
    }

    public void testOnGenerationalFileDeletion() throws Exception {
        var indexNode = startMasterAndIndexNode(
            Settings.builder().put(StatelessCommitService.STATELESS_GENERATIONAL_FILES_TRACKING_ENABLED.getKey(), true).build()
        );
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
        var searchDirectory = SearchDirectory.unwrapDirectory(directory);
        assertBusy(() -> {
            for (var expected : expectedLocations.entrySet()) {
                var fileName = expected.getKey();
                var blobLocation = SearchDirectoryTestUtils.getBlobLocation(searchDirectory, fileName);
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
            try (var searcher = searchEngine.acquireSearcher("search_engine_directory_reader_generation")) {
                assertThat(searcher.getDirectoryReader().getIndexCommit().getGeneration(), matcher);
            }
        });
    }

    private static void assertBusyOpenedGenerationalFiles(Directory directory, Map<String, Long> generationalFiles) throws Exception {
        var trackingDirectory = GenerationalFilesTrackingSearchDirectory.unwrapTrackingDirectory(directory);
        assertThat(trackingDirectory, notNullValue());
        assertBusy(() -> assertThat(trackingDirectory.getGenerationalFilesAsMap(), equalTo(generationalFiles)));
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
