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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.lucene.SearchIndexInput;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class GenerationalDocValuesIT extends AbstractStatelessIntegTestCase {

    private static Set<IndexInput> inputs = ConcurrentCollections.newConcurrentSet();

    /**
     * A plugin that tracks {@link IndexInput} instances opened by a {@link SearchDirectory}
     */
    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected SearchDirectory createSearchDirectory(
            StatelessSharedBlobCacheService cacheService,
            CacheBlobReaderService cacheBlobReaderService,
            MutableObjectStoreUploadTracker objectStoreUploadTracker,
            ShardId shardId
        ) {
            return new TrackingSearchDirectory(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId, inputs);
        }

        private static class TrackingSearchDirectory extends SearchDirectory {

            private final Set<IndexInput> inputs;

            TrackingSearchDirectory(
                StatelessSharedBlobCacheService cacheService,
                CacheBlobReaderService cacheBlobReaderService,
                MutableObjectStoreUploadTracker objectStoreUploadTracker,
                ShardId shardId,
                Set<IndexInput> inputs
            ) {
                super(cacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
                this.inputs = Objects.requireNonNull(inputs);
            }

            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                var searchInput = super.openInput(name, context);
                return new TrackingSearchIndexInput(inputs, searchInput);
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
        inputs.clear();
    }

    public void testBlobLocationsUpdates() throws Exception {
        startMasterOnlyNode();
        startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );

        final String indexName = getTestName().toLowerCase(Locale.ROOT);
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
        final long primaryTerm = indexShard.getOperationPrimaryTerm();

        executeBulk(bulkRequest -> {
            // create 1000 docs with ids {0..999} in segment core _0
            IntStream.range(0, 1_000)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("text", randomAlphaOfLength(10))));
        });
        flush(indexName);

        var expectedLocations = Map.ofEntries(
            entry("segments_4", 4L),
            // new segment core _0
            entry("_0.cfe", 4L),
            entry("_0.cfs", 4L),
            entry("_0.si", 4L)
        );

        assertThat(indexEngine.getCurrentGeneration(), equalTo(4L));
        assertBusyFilesLocations(indexDirectory, expectedLocations);

        executeBulk(bulkRequest -> {
            // delete 10 docs with ids {0..9} in segment core _0
            IntStream.range(0, 10).forEach(i -> bulkRequest.add(new DeleteRequest(indexName).id(String.valueOf(i))));
            // create 10 docs with ids {10,000..10,009} in segment core _1
            IntStream.range(10_000, 10_010)
                .forEach(i -> bulkRequest.add(new IndexRequest(indexName).id(String.valueOf(i)).source("text", randomAlphaOfLength(10))));
        });
        flush(indexName);

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

        assertThat(indexEngine.getCurrentGeneration(), equalTo(5L));
        assertBusyFilesLocations(indexDirectory, expectedLocations);

        // compound commit for generation 5 exists in the object store
        final String compoundCommitBlobName_5 = StatelessCompoundCommit.blobNameFromGeneration(5L);
        assertBusy(() -> {
            var blobs = indexDirectory.getSearchDirectory().getBlobContainer(primaryTerm).listBlobs(OperationPurpose.INDICES).keySet();
            assertThat(blobs, hasItem(compoundCommitBlobName_5));
        });

        // now start a search shard on commit generation 5
        startSearchNode(
            Settings.builder()
                .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "4kb")
                .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "32kb")
                .build()
        );
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());

        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(5L));
        assertBusyFilesLocations(searchDirectory, expectedLocations);

        // open a searcher on generation 5 which includes segment cores _0, _1 and generational files of _0
        var searcher_5 = searchShard.acquireSearcher("searcher_generation_5");
        assertThat(searcher_5.getDirectoryReader().getIndexCommit().getGeneration(), equalTo(5L));
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(new PrimaryTermAndGeneration(primaryTerm, 4L), new PrimaryTermAndGeneration(primaryTerm, 5L))
        );

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

        assertThat(indexEngine.getCurrentGeneration(), equalTo(6L));
        assertBusyFilesLocations(indexDirectory, expectedLocations);
        assertBusyFilesLocations(searchDirectory, expectedLocations);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(6L));
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(
                new PrimaryTermAndGeneration(primaryTerm, 4L),
                new PrimaryTermAndGeneration(primaryTerm, 5L),
                new PrimaryTermAndGeneration(primaryTerm, 6L)
            )
        );

        // now force-merge to 2 segments: segment cores _1 and _2 should be merged together as they are smaller than _0
        var forceMerge = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(2).setFlush(false).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(2));
        refresh(indexName);

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

        assertThat(indexEngine.getCurrentGeneration(), equalTo(7L));
        // The indexDirectory updates its searchDirectory on upload.
        // Since refresh does not upload, we skip assertions for its searchDirectory.
        assertDirectoryContents(indexDirectory, expectedLocations);
        assertBusyFilesLocations(searchDirectory, expectedLocations);
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(7L));
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            contains(new PrimaryTermAndGeneration(primaryTerm, 4L), new PrimaryTermAndGeneration(primaryTerm, 7L))
        );

        // There is also a bug in ref counting that prevents the deletion of the stateless_commit_5 files, so we delete it manually for now:
        // TODO https://github.com/elastic/elasticsearch-serverless/pull/1165
        indexDirectory.getSearchDirectory()
            .getBlobContainer(primaryTerm)
            .deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, List.of(compoundCommitBlobName_5).iterator());

        assertBusy(() -> {
            var blobs = indexDirectory.getSearchDirectory().getBlobContainer(primaryTerm).listBlobs(OperationPurpose.INDICES).keySet();
            assertThat(blobs, not(hasItem(compoundCommitBlobName_5)));
        });

        // clear the shared cache to force reading data from object store again
        var searchCacheService = SearchDirectoryTestUtils.getCacheService(searchDirectory);
        searchCacheService.forceEvict(fileCacheKey -> compoundCommitBlobName_5.equals(fileCacheKey.fileName()));

        for (var input : inputs) {
            FileCacheKey cacheKey = null;
            if (input instanceof SearchIndexInput searchInput) {
                cacheKey = SearchDirectoryTestUtils.getCacheKey(searchInput);
            }
            assertThat("No cache key found for input " + input, cacheKey, notNullValue());
            if (cacheKey.fileName().equals(compoundCommitBlobName_5)) {
                // seek to the end of the file and restore position to make sure we flushed all internal buffers for existing index inputs
                long pos = input.getFilePointer();
                input.seek(input.length());
                input.seek(pos);
            }
        }
        Exception e = expectThrows(Exception.class, () -> {
            try (var searcher_7 = searchShard.acquireSearcher("searcher_generation_7")) {
                assertThat(searcher_7.getDirectoryReader().getIndexCommit().getGeneration(), equalTo(7L));
                // make sure to read the generation files to trigger a cache miss
                readGenerationalDocValues(searcher_7);
            }
        });
        var cause = ExceptionsHelper.unwrap(e, NoSuchFileException.class);
        assertThat(cause, notNullValue());
        assertThat(cause.getMessage(), containsString("stateless_commit_5"));

        // look into opened SearchIndexInput to see which FileCacheKey they are using
        List<IndexInput> orphanIndexInputs = new ArrayList<>();
        for (var input : inputs) {
            FileCacheKey cacheKey = null;
            if (input instanceof SearchIndexInput searchInput) {
                cacheKey = SearchDirectoryTestUtils.getCacheKey(searchInput);
            }
            assertThat("No cache key found for input " + input, cacheKey, notNullValue());
            if (cacheKey.fileName().equals(compoundCommitBlobName_5)) {
                orphanIndexInputs.add(input);
            }
        }

        // We should have an empty collection of IndexInput that use the compoundCommitBlobName_5
        // TODO https://elasticco.atlassian.net/browse/ES-6563
        assertThat(
            "Fix this: one or more opened SearchIndexInput is still using a cache key pointing to stateless_commit_5",
            orphanIndexInputs,
            not(empty())
        );
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
