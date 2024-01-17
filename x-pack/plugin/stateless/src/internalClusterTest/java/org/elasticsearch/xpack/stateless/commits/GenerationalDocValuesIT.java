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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.lucene.SearchIndexInput;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchResponseUtils;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class GenerationalDocValuesIT extends AbstractStatelessIntegTestCase {

    private static Set<IndexInput> generationalDocValuesInputs = ConcurrentCollections.newConcurrentSet(); // both index and search shards
    private static Set<PrimaryTermAndGeneration> indexingGenerationalFilesClosed = ConcurrentCollections.newConcurrentSet(); // index shards

    /**
     * A plugin that tracks {@link IndexInput} instances of generational doc values opened by a {@link SearchDirectory}
     */
    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected SearchDirectory createSearchDirectory(
            SharedBlobCacheService<FileCacheKey> cacheService,
            ShardId shardId,
            Consumer<PrimaryTermAndGeneration> generationalFilesClosed
        ) {
            if (generationalFilesClosed != null) {
                generationalFilesClosed = generationalFilesClosed.andThen(termGen -> indexingGenerationalFilesClosed.add(termGen));
            }
            return new TrackingSearchDirectory(cacheService, shardId, generationalFilesClosed, generationalDocValuesInputs);
        }

        private static class TrackingSearchDirectory extends SearchDirectory {

            private final Set<IndexInput> generationalDocValuesInputs;

            TrackingSearchDirectory(
                SharedBlobCacheService<FileCacheKey> cacheService,
                ShardId shardId,
                Consumer<PrimaryTermAndGeneration> generationalFilesClosed,
                Set<IndexInput> generationalDocValuesInputs
            ) {
                super(cacheService, shardId, generationalFilesClosed);
                this.generationalDocValuesInputs = Objects.requireNonNull(generationalDocValuesInputs);
            }

            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                var searchInput = super.openInput(name, context);
                if (StatelessCommitService.isGenerationalFile(name)) {
                    return new TrackingSearchIndexInput(generationalDocValuesInputs, searchInput);
                } else {
                    return searchInput;
                }
            }
        }

        private static class TrackingSearchIndexInput extends FilterIndexInput {

            private final Set<IndexInput> inputs;
            private final AtomicBoolean closed;

            TrackingSearchIndexInput(Set<IndexInput> inputs, IndexInput in) {
                super("active", in);
                this.inputs = Objects.requireNonNull(inputs);
                this.closed = new AtomicBoolean();
                this.inputs.add(in);
            }

            @Override
            public void close() throws IOException {
                if (closed.compareAndSet(false, true)) {
                    inputs.remove(in);
                    super.close();
                }
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return plugins;
    }

    @Before
    public void clearInputs() {
        generationalDocValuesInputs.clear();
        indexingGenerationalFilesClosed.clear();
    }

    public void testBlobLocationsUpdates() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder()
            .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .build();
        String indexNode = startIndexNode(indexNodeSettings);

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "tiered")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .build()
        );
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var indexShard = findIndexShard(index, 0);
        final var indexEngine = getShardEngine(indexShard, IndexEngine.class);
        final var indexDirectory = IndexDirectory.unwrapDirectory(indexShard.store().directory());
        final PrimaryTermAndGeneration gen3 = getPrimaryTermAndGeneration(indexName);

        executeBulk(bulkRequest -> {
            // create 1000 docs with ids {0..999} in segment core _0
            IntStream.range(0, 1_000)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("text", randomAlphaOfLength(10))));
        });
        flush(indexName);
        final PrimaryTermAndGeneration gen4 = getPrimaryTermAndGeneration(indexName);

        var expectedLocations = Map.ofEntries(
            entry("segments_4", 4L),
            // new segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(4L));
        assertBusyFilesLocations(indexName, true, expectedLocations);

        executeBulk(bulkRequest -> {
            // delete 10 docs with ids {0..9} in segment core _0
            IntStream.range(0, 10).forEach(i -> bulkRequest.add(new DeleteRequest(indexName).id(String.valueOf(i))));
            // create 10 docs with ids {10,000..10,009} in segment core _1
            IntStream.range(10_000, 10_010)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("text", randomAlphaOfLength(10))));
        });
        flush(indexName);
        final PrimaryTermAndGeneration gen5 = getPrimaryTermAndGeneration(indexName);

        expectedLocations = Map.ofEntries(
            entry("segments_5", 5L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // new generational files for _0
            entry("_0_1.fnm", 5L),
            entry("_0_1_Lucene90_0.dvd", 5L),
            entry("_0_1_Lucene90_0.dvm", 5L),
            // new segment core _1
            entry("_1.cfe", 5L),
            entry("_1.cfs", 5L),
            entry("_1.si", 5L)
        );

        assertThat(gen5.generation(), equalTo(5L));
        assertBusyFilesLocations(indexName, true, expectedLocations);

        // compound commit for generation 5 exists in the object store
        final String compoundCommitBlobName_5 = StatelessCompoundCommit.blobNameFromGeneration(gen5.generation());
        assertBusy(() -> {
            var blobs = indexDirectory.getSearchDirectory()
                .getBlobContainer(gen5.primaryTerm())
                .listBlobs(OperationPurpose.INDICES)
                .keySet();
            assertThat(blobs, hasItem(compoundCommitBlobName_5));
        });

        // now start a search shard on commit generation 5
        var searchNodeSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "4kb")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "32kb")
            .build();
        String searchNode = startSearchNode(searchNodeSettings);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var searchShard = findSearchShard(index, 0);
        var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());

        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(gen5.generation()));
        assertBusyFilesLocations(indexName, false, expectedLocations);

        // open a searcher on generation 5 which includes segment cores _0, _1 and generational files of _0
        var searcher_5 = searchShard.acquireSearcher("searcher_generation_5");
        assertThat(searcher_5.getDirectoryReader().getIndexCommit().getGeneration(), equalTo(gen5.generation()));
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(gen5));

        long totalLiveDocs = readGenerationalDocValues(searcher_5);
        assertThat("10 docs deleted, 10 docs created", totalLiveDocs, equalTo(1000L - 10L + 10L));

        // close the searcher to release commit with generation 5
        searcher_5.close();

        executeBulk(bulkRequest -> {
            // create 10 docs with ids {10,010..10,019} in segment core _2
            IntStream.range(10_010, 10_020)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("text", randomAlphaOfLength(10))));
        });
        flush(indexName);
        final PrimaryTermAndGeneration gen6 = getPrimaryTermAndGeneration(indexName);

        expectedLocations = Map.ofEntries(
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
            // updated locations for generational files of _0
            entry("_0_1.fnm", 6L),
            entry("_0_1_Lucene90_0.dvd", 6L),
            entry("_0_1_Lucene90_0.dvm", 6L)
        );

        assertThat(gen6.generation(), equalTo(6L));
        assertBusyFilesLocations(indexName, true, expectedLocations);
        assertBusyFilesLocations(indexName, false, expectedLocations);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(gen6.generation()));
        // gen5 generational files are still in use, while gen6 generational files (even if unused) are the latest copy
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), containsInAnyOrder(gen5, gen6));
        assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(gen3, gen4));

        // now force-merge to 2 segments: segment cores _1 and _2 should be merged together as they are smaller than _0
        var forceMerge = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(2).setFlush(false).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(2));
        refresh(indexName);
        final PrimaryTermAndGeneration gen7 = getPrimaryTermAndGeneration(indexName);

        expectedLocations = Map.ofEntries(
            entry("segments_7", 7L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // new segment core _3
            entry("_3.cfe", 7L),
            entry("_3.cfs", 7L),
            entry("_3.si", 7L),
            // updated locations for generational files of _0
            entry("_0_1.fnm", 7L),
            entry("_0_1_Lucene90_0.dvd", 7L),
            entry("_0_1_Lucene90_0.dvm", 7L)
        );

        assertThat(gen7.generation(), equalTo(7L));
        assertBusyFilesLocations(indexName, true, expectedLocations);
        assertBusyFilesLocations(indexName, false, expectedLocations);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(gen7.generation()));
        // Generation 5 is still acquired due to the open generational doc values file
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), containsInAnyOrder(gen5, gen7));
        assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(gen3, gen4, gen6));

        // Create an extra commit to avoid any potential occurrences of https://elasticco.atlassian.net/browse/ES-7336
        refresh(indexName);
        final PrimaryTermAndGeneration gen8 = getPrimaryTermAndGeneration(indexName);
        assertThat(gen8.generation(), equalTo(8L));

        expectedLocations = Map.ofEntries(
            entry("segments_8", 8L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // referenced segment core _3
            entry("_3.cfe", 7L),
            entry("_3.cfs", 7L),
            entry("_3.si", 7L),
            // updated locations for generational files of _0
            entry("_0_1.fnm", 8L),
            entry("_0_1_Lucene90_0.dvd", 8L),
            entry("_0_1_Lucene90_0.dvm", 8L)
        );
        assertBusyFilesLocations(indexName, true, expectedLocations);
        assertBusyFilesLocations(indexName, false, expectedLocations);

        // Generation 5 is still acquired due to the open generational doc values file
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), containsInAnyOrder(gen5, gen8));
        // at least 1 generational file open on index and search nodes
        assertThat(generationalDocValuesInputs.size(), greaterThanOrEqualTo(2));
        for (var it = generationalDocValuesInputs.iterator(); it.hasNext();) {
            var refCountedBlobLocation = SearchDirectoryTestUtils.getRefCountedBlobLocation((SearchIndexInput) it.next());
            assertThat(refCountedBlobLocation.getBlobLocation().compoundFileGeneration(), equalTo(5L));
        }
        assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(gen3, gen4, gen6, gen7));

        // Restart the search node, or relocate the search shard, on commit generation 8
        boolean restartOrRelocateSearchNode = randomBoolean();
        if (restartOrRelocateSearchNode) {
            internalCluster().restartNode(searchNode);
        } else {
            startSearchNode(searchNodeSettings);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode), indexName);
            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNode))));
        }
        ensureGreen(indexName);
        searchShard = findSearchShard(index, 0);
        searchEngine = getShardEngine(searchShard, SearchEngine.class);

        // Note that the generational doc values files should be opened from their latest copies in generation 8
        assertBusyFilesLocations(indexName, true, expectedLocations);
        assertBusyFilesLocations(indexName, false, expectedLocations);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(gen8.generation()));
        // Generation 5 is not acquired anymore since the re-opened search engine uses the latest copy of the generational doc values file
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(gen8));
        assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(gen3, gen4, gen6, gen7));
        // Ensure which stateless compound commits in the object store are deleted and which are still there
        assertBusy(() -> assertThat(getBlobCommits(indexShard.shardId()), containsInAnyOrder(gen4, gen5, gen7, gen8)));

        // Restart the index node, or relocate the indexing shard, on commit generation 8.
        // The indexing shard, when recovering, also forces a flush, with a new generation 9.
        boolean restartOrRelocateIndexNode = randomBoolean();
        if (restartOrRelocateIndexNode) {
            internalCluster().restartNode(indexNode);
        } else {
            startIndexNode(indexNodeSettings);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode), indexName);
            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNode))));
        }
        ensureGreen(indexName);
        searchShard = findSearchShard(index, 0);
        searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final PrimaryTermAndGeneration gen9 = getPrimaryTermAndGeneration(indexName);
        assertThat(gen9.generation(), equalTo(9L));

        expectedLocations = Map.ofEntries(
            entry("segments_9", 9L),
            // referenced segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L),
            // referenced segment core _3
            entry("_3.cfe", 7L),
            entry("_3.cfs", 7L),
            entry("_3.si", 7L),
            // updated locations for generational files of _0
            entry("_0_1.fnm", 9L),
            entry("_0_1_Lucene90_0.dvd", 9L),
            entry("_0_1_Lucene90_0.dvm", 9L)
        );

        assertBusyFilesLocations(indexName, true, expectedLocations);
        assertBusyFilesLocations(indexName, false, expectedLocations);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(gen9.generation()));
        if (restartOrRelocateIndexNode) {
            // When the index node is restarted, the search shard is also re-allocated, and reads the latest copy of the generational files.
            assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(gen9));
        } else {
            // When the index shard is relocated, the search shard updates the commit, and the old generational files are still in
            // use. The latest copy of generational files also appear as acquired.
            assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), containsInAnyOrder(gen8, gen9));
        }

        assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(gen3, gen4, gen5, gen6, gen7, gen8)); // gen8 closed by original node
        // Check that new node has gen8 still in use due to the generational files being opened during recovery from gen8
        final var newIndexShard = findIndexShard(index, 0);
        final var newIndexEngine = getShardEngine(newIndexShard, IndexEngine.class);
        assertTrue(newIndexEngine.getStatelessCommitService().hasClosedGenerationalFiles(newIndexShard.shardId(), gen4));
        assertTrue(newIndexEngine.getStatelessCommitService().hasClosedGenerationalFiles(newIndexShard.shardId(), gen7));
        // these are still in use when the indexing shard recovered from generation 8
        assertFalse(newIndexEngine.getStatelessCommitService().hasClosedGenerationalFiles(newIndexShard.shardId(), gen8));
        // even if unused, they are the latest generational files tracked
        assertFalse(newIndexEngine.getStatelessCommitService().hasClosedGenerationalFiles(newIndexShard.shardId(), gen9));
        // Ensure which stateless compound commits in the object store are deleted and which are still there
        assertBusy(() -> assertThat(getBlobCommits(newIndexShard.shardId()), containsInAnyOrder(gen4, gen7, gen8, gen9)));
    }

    public void testAcquiredGenerationsWhenHoldingReferenceToGenerationalFile() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "tiered")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .build()
        );
        ensureGreen(indexName);

        final PrimaryTermAndGeneration genA = getPrimaryTermAndGeneration(indexName);
        final var index = resolveIndex(indexName);
        final var indexShard = findIndexShard(index, 0);
        final SearchDirectory indexNodeSearchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());

        startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        final var searchShard = findSearchShard(index, 0);
        final SearchDirectory searchNodeSearchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());

        int initialDocs = randomIntBetween(10, 100);
        executeBulk(bulkRequest -> {
            IntStream.range(0, initialDocs)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("text", randomAlphaOfLength(10))));
        });
        refresh(indexName);
        final PrimaryTermAndGeneration genB = getPrimaryTermAndGeneration(indexName);
        assertEquals(initialDocs, SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())));

        int docsToDelete = randomIntBetween(1, initialDocs);
        executeBulk(bulkRequest -> {
            IntStream.range(0, docsToDelete).forEach(i -> bulkRequest.add(new DeleteRequest(indexName).id(String.valueOf(i))));
        });
        refresh(indexName); // generational files here will be used from now on
        final PrimaryTermAndGeneration genC = getPrimaryTermAndGeneration(indexName);
        refresh(indexName); // refresh second time so we have an unused copy of the generational files to hold a reference from
        final PrimaryTermAndGeneration genD = getPrimaryTermAndGeneration(indexName);
        assertEquals(
            initialDocs - docsToDelete,
            SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()))
        );
        assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(genA, genB));

        // Hold a reference to a generational file of genD in both the indexing and the search shards
        var indexShardEngineOrNull = indexShard.getEngineOrNull();
        assertThat(indexShardEngineOrNull, instanceOf(IndexEngine.class));
        var indexEngine = (IndexEngine) indexShardEngineOrNull;
        String genDFilename = Lucene.getIndexCommit(indexEngine.getLastCommittedSegmentInfos(), indexNodeSearchDirectory)
            .getFileNames()
            .stream()
            .filter(StatelessCommitService::isGenerationalFile)
            .findAny()
            .get();
        var searchShardEngineOrNull = searchShard.getEngineOrNull();
        assertThat(searchShardEngineOrNull, instanceOf(SearchEngine.class));
        var searchEngine = (SearchEngine) searchShardEngineOrNull;
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), containsInAnyOrder(genC, genD));
        try (
            // Opening an IndexInput to hold a reference. This can happen e.g. if snapshot logic is reading the files.
            IndexInput indexInput = indexNodeSearchDirectory.openInput(genDFilename, IOContext.READONCE);
            InputStream indexLocal = new InputStreamIndexInput(indexInput, indexInput.length());
            IndexInput searchInput = searchNodeSearchDirectory.openInput(genDFilename, IOContext.READONCE);
            InputStream searchLocal = new InputStreamIndexInput(searchInput, searchInput.length());
        ) {
            // New commit with genE
            int moreDocs = randomIntBetween(10, 100);
            executeBulk(bulkRequest -> {
                IntStream.range(0, moreDocs)
                    .forEach(
                        i -> bulkRequest.add(
                            new IndexRequest(indexName).id(String.valueOf(initialDocs + i)).source("text", randomAlphaOfLength(10))
                        )
                    );
            });
            refresh(indexName);
            final PrimaryTermAndGeneration genE = getPrimaryTermAndGeneration(indexName);
            // Acquired gens are genC (generational files in use), genD (we hold a reference), and genE (latest copy of generational files)
            assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), containsInAnyOrder(genC, genD, genE));
            assertThat(indexingGenerationalFilesClosed, containsInAnyOrder(genA, genB));
        }

        final PrimaryTermAndGeneration genE = getPrimaryTermAndGeneration(indexName);
        assertThat(
            "Acquired generations should not hold " + genD + " anymore since we released the reference to the generational file",
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            containsInAnyOrder(genC, genE)
        );
        assertThat(
            "Indexing node's closed generations should not hold "
                + genD
                + " anymore since we released the reference to the generational file",
            indexingGenerationalFilesClosed,
            containsInAnyOrder(genA, genB, genD)
        );
    }

    @SuppressWarnings("unchecked")
    private static <E extends Engine> E getShardEngine(IndexShard indexShard, Class<E> engineClass) {
        var engine = indexShard.getEngineOrNull();
        assertThat(engine, notNullValue());
        assertThat(engine, instanceOf(engineClass));
        return (E) engine;
    }

    private static void executeBulk(Consumer<BulkRequestBuilder> consumer) {
        var bulkRequest = client().prepareBulk();
        consumer.accept(bulkRequest);
        assertNoFailures(bulkRequest.get());
    }

    private static void assertBusyFilesLocations(String indexName, boolean indexOrSearchNode, Map<String, Long> expectedLocations)
        throws Exception {
        final var index = resolveIndex(indexName);
        final Directory directory = indexOrSearchNode
            ? IndexDirectory.unwrapDirectory(findIndexShard(index, 0).store().directory())
            : SearchDirectory.unwrapDirectory(findSearchShard(index, 0).store().directory());
        var searchDirectory = SearchDirectory.unwrapDirectory(directory);
        assertBusy(() -> {
            for (var expected : expectedLocations.entrySet()) {
                var fileName = expected.getKey();
                var refCountedBlobLocation = SearchDirectoryTestUtils.getRefCountedBlobLocation(searchDirectory, fileName);
                var actualCompoundCommitGeneration = refCountedBlobLocation != null
                    ? refCountedBlobLocation.getBlobLocation().compoundFileGeneration()
                    : null;
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
            assertThat(List.of(directory.listAll()), equalTo(expectedLocations.keySet().stream().sorted(String::compareTo).toList()));
        });
    }

    private static PrimaryTermAndGeneration getPrimaryTermAndGeneration(String indexName) {
        final var indexShard = findIndexShard(resolveIndex(indexName), 0);
        final var engineOrNull = indexShard.getEngineOrNull();
        assertThat(engineOrNull, notNullValue());
        return new PrimaryTermAndGeneration(indexShard.getOperationPrimaryTerm(), ((IndexEngine) engineOrNull).getCurrentGeneration());
    }

    private static Set<PrimaryTermAndGeneration> getBlobCommits(ShardId shardId) throws Exception {
        Set<PrimaryTermAndGeneration> set = new HashSet<>();
        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, internalCluster().getRandomNodeName());
        var indexBlobContainer = objectStoreService.getBlobContainer(shardId);
        for (var entry : indexBlobContainer.children(operationPurpose).entrySet()) {
            var primaryTerm = Long.parseLong(entry.getKey());
            Set<String> statelessCompoundCommits = entry.getValue().listBlobs(operationPurpose).keySet();
            statelessCompoundCommits.forEach(
                filename -> set.add(
                    new PrimaryTermAndGeneration(primaryTerm, StatelessCompoundCommit.parseGenerationFromBlobName(filename))
                )
            );
        }
        return set;
    }

    /**
     * Makes sure to read the generational files (doc by doc) and returns the total number of live docs, without closing the Searcher.
     */
    private static long readGenerationalDocValues(Engine.Searcher searcher_5) throws IOException {
        long totalLiveDocs = 0L;
        for (var leaf : searcher_5.getDirectoryReader().leaves()) {
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
