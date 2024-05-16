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
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import static co.elastic.elasticsearch.stateless.lucene.SearchDirectoryTestUtils.getCacheService;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.action.search.SearchTransportService.QUERY_ACTION_NAME;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class VirtualBatchedCompoundCommitsIT extends AbstractStatelessIntegTestCase {

    /**
     * A plugin that:
     * <ul>
     *   <li>Gives the ability to override the
     *       {@link IndexEngine#readVirtualBatchedCompoundCommitChunk(GetVirtualBatchedCompoundCommitChunkRequest, StreamOutput)} function
     *       on an indexing node to produce file not found failures when given offset is of a specific value (Long.MAX_VALUE)</li>
     *   <li>Prevents VBCC automatic uploads (uploads can be manually triggered).</li>
     *   <li>Disables pre warming.</li>
     * </ul>
     */
    public static class TestStateless extends Stateless {
        public volatile TestStatelessCommitService statelessCommitService;

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            DocumentParsingProvider documentParsingProvider
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                refreshThrottlerFactory,
                statelessCommitService.getIndexEngineLocalReaderListenerForShard(engineConfig.getShardId()),
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider
            ) {
                @Override
                public void readVirtualBatchedCompoundCommitChunk(
                    final GetVirtualBatchedCompoundCommitChunkRequest request,
                    final StreamOutput output
                ) throws IOException {
                    if (request.getOffset() == Long.MAX_VALUE) {
                        throw randomFrom(new FileNotFoundException("simulated"), new NoSuchFileException("simulated"));
                    }
                    super.readVirtualBatchedCompoundCommitChunk(request, output);
                }
            };
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
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
            statelessCommitService = new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                client,
                commitCleaner,
                cacheWarmingService
            );
            return statelessCommitService;
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            Settings settings
        ) {
            return new NoopSharedBlobCacheWarmingService(cacheService, threadPool, settings);
        }
    }

    public static class TestStatelessCommitService extends StatelessCommitService {

        public TestStatelessCommitService(
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
        protected ShardCommitState createShardCommitState(
            ShardId shardId,
            long primaryTerm,
            BooleanSupplier inititalizingNoSearchSupplier
        ) {
            return new ShardCommitState(shardId, primaryTerm, inititalizingNoSearchSupplier) {
                @Override
                protected boolean shouldUploadVirtualBcc(VirtualBatchedCompoundCommit virtualBcc) {
                    // uploads must be triggered explicitly in tests
                    return false;
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1));
    }

    private void evictSearchShardCache(String indexName) {
        IndexShard shard = findSearchShard(indexName);
        Directory directory = shard.store().directory();
        SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(directory);
        getCacheService(searchDirectory).forceEvict((key) -> true);
    }

    private record IndexedDocs(int numDocs, int customDocs, long sum) {}

    /**
     * Indexes a number of documents until the directory has a size of at least a few pages, and refreshes the index. The mappings are:
     * - "field" with a random string of length between 1 and 25
     * - "number" with a random long between 0 and the current index
     * - "custom" with a value "value" for customDocs number of docs. customDocs will be less or equal to numDocs.
     *
     * @return a {@link IndexedDocs} object with the number of docs, the number of custom docs and the sum of the numbers.
     */
    private IndexedDocs indexDocsAndRefresh(String indexName) throws Exception {
        int totalDocs = 0;
        int totalCustomDocs = 0;
        long sum = 0;
        long pages = randomLongBetween(4, 8);

        assertThat("this function only works for 1 shard indices", getNumShards(indexName).numPrimaries, equalTo(1));
        var shard = findIndexShard(indexName);
        var directory = shard.store().directory();

        do {
            final int newDocs = randomIntBetween(10, 20);
            totalDocs += newDocs;
            int newCustomDocs = randomIntBetween(1, 10);
            totalCustomDocs += newCustomDocs;
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < newDocs; i++) {
                long number = randomLongBetween(0, i);
                sum += number;
                if (newCustomDocs-- > 0) {
                    bulkRequest.add(
                        new IndexRequest(indexName).source(
                            "field",
                            randomUnicodeOfCodepointLengthBetween(1, 25),
                            "number",
                            number,
                            "custom",
                            "value"
                        )
                    );
                } else {
                    bulkRequest.add(
                        new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25), "number", number)
                    );
                }
            }
            assertNoFailures(bulkRequest.get());
        } while (getDirectorySize(directory) <= PAGE_SIZE * pages);

        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        logger.info(
            "--> indexed {} docs in {} with {} custom docs, sum {}, directory size {}",
            totalDocs,
            indexName,
            totalCustomDocs,
            sum,
            getDirectorySize(directory)
        );
        return new IndexedDocs(totalDocs, totalCustomDocs, sum);
    }

    private long getDirectorySize(Directory directory) throws IOException {
        long size = 0;
        for (String file : directory.listAll()) {
            size += directory.fileLength(file);
        }
        return size;
    }

    private enum TestSearchType {
        MATCH_ALL,
        MATCH_CUSTOM,
        SUM
    }

    static SearchRequestBuilder prepareSearch(String indexName, TestSearchType testSearchType) {
        return switch (testSearchType) {
            case MATCH_ALL -> prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
            case MATCH_CUSTOM -> prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value"));
            case SUM -> prepareSearch(indexName).addAggregation(sum("sum").field("number"));
        };
    }

    static Consumer<SearchResponse> getSearchResponseValidator(TestSearchType testSearchType, IndexedDocs indexedDocs) {
        // We avoid setSize(0) with the below statements, because we want the searches to fetch some chunks.
        return switch (testSearchType) {
            case MATCH_ALL -> searchResponse -> assertEquals(indexedDocs.numDocs, searchResponse.getHits().getTotalHits().value);
            case MATCH_CUSTOM -> searchResponse -> assertEquals(indexedDocs.customDocs, searchResponse.getHits().getTotalHits().value);
            case SUM -> searchResponse -> {
                Sum sum = searchResponse.getAggregations().get("sum");
                assertEquals(indexedDocs.sum, sum.value(), 0.001);
            };
        };
    }

    static void validateSearchResponse(String indexName, TestSearchType testSearchType, IndexedDocs indexedDocs) {
        assertNoFailuresAndResponse(prepareSearch(indexName, testSearchType), getSearchResponseValidator(testSearchType, indexedDocs));
    }

    public void testGetVirtualBatchedCompoundCommitChunk() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var shardId = findIndexShard(indexName).shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var vbccGen = indexShardCommitService.getCurrentVirtualBcc(shardId).getPrimaryTermAndGeneration().generation();

        // serve Lucene files from the indexing node
        if (randomBoolean()) {
            evictSearchShardCache(indexName);
        }

        TestSearchType testSearchType = randomFrom(TestSearchType.values());
        validateSearchResponse(indexName, testSearchType, indexedDocs);

        // Ensure VBCC not yet uploaded
        assertThat(indexShardCommitService.getCurrentVirtualBcc(shardId).getPrimaryTermAndGeneration().generation(), equalTo(vbccGen));

        // Upload VBCC
        flush(indexName);
        assertNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        // Search via the object store
        if (randomBoolean()) {
            evictSearchShardCache(indexName);
        }
        validateSearchResponse(indexName, testSearchType, indexedDocs);
    }

    public void testGetVirtualBatchedCompoundCommitChunkDuringPrimaryRelocation() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        var searchNode = startSearchNode();
        final var indexNodeB = startIndexNode();
        ensureStableCluster(4);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        final var indexNodeTransportService = MockTransportService.getInstance(indexNodeA);
        CountDownLatch relocationStarted = new CountDownLatch(1);
        CountDownLatch searchSent = new CountDownLatch(1);
        indexNodeTransportService.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
            relocationStarted.countDown();
            safeAwait(searchSent);
            handler.messageReceived(request, channel, task);
        });

        logger.info("--> excluding {}", indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(relocationStarted);

        if (randomBoolean()) {
            evictSearchShardCache(indexName);
        }

        final var searchNodeTransportService = MockTransportService.getInstance(searchNode);
        searchNodeTransportService.addRequestHandlingBehavior(QUERY_ACTION_NAME, (handler, request, channel, task) -> {
            handler.messageReceived(request, channel, task);
            searchSent.countDown();
        });

        var thread = new Thread(() -> {
            // serve Lucene files from the indexing node
            TestSearchType testSearchType = randomFrom(TestSearchType.values());
            validateSearchResponse(indexName, testSearchType, indexedDocs);
        });
        thread.start();

        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        thread.join();
    }

    public void testGetVirtualBatchedCompoundCommitChunkRetriesIfPrimaryRelocates() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        var searchNode = startSearchNode();
        final var indexNodeB = startIndexNode();
        ensureStableCluster(4);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        CountDownLatch actionAppeared = new CountDownLatch(1);
        CountDownLatch actionSent = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (connection.getNode().getName().equals(indexNodeA)
                && action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                actionAppeared.countDown();
                safeAwait(actionSent);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Empty cache on search node, to ensure an action is sent to the indexing node
        evictSearchShardCache(indexName);

        var thread = new Thread(() -> {
            // serve Lucene files from the indexing node
            TestSearchType testSearchType = randomFrom(TestSearchType.values());
            validateSearchResponse(indexName, testSearchType, indexedDocs);
        });
        thread.start();

        safeAwait(actionAppeared);
        // Relocate primary, which will make the action retry due to the IndexNotFoundException on the original indexing node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        awaitClusterState(
            logger,
            indexNodeA,
            clusterState -> clusterState.routingTable()
                .index(indexName)
                .shard(shardId.id())
                .primaryShard()
                .currentNodeId()
                .equals(getNodeId(indexNodeB))
        );
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        logger.info("relocated primary");

        final var indexNodeATransportService = MockTransportService.getInstance(indexNodeA);
        indexNodeATransportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                @Override
                public void sendResponse(Exception exception) {
                    assertThat(exception, instanceOf(IndexNotFoundException.class));
                    channel.sendResponse(exception);
                }

                @Override
                public String getProfileName() {
                    return channel.getProfileName();
                }

                @Override
                public void sendResponse(TransportResponse response) {
                    assert false : "unexpectedly trying to send response " + response;
                }
            }, task)
        );

        final var indexNodeBTransportService = MockTransportService.getInstance(indexNodeB);
        CountDownLatch actionOnNewPrimarySeen = new CountDownLatch(1);
        indexNodeBTransportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                actionOnNewPrimarySeen.countDown();
                handler.messageReceived(request, channel, task);
            }
        );

        actionSent.countDown();
        safeAwait(actionOnNewPrimarySeen);
        thread.join();
    }

    private enum FailureType {
        INDEX_DELETED,
        INDEX_CLOSED
    }

    private void testGetVirtualBatchedCompoundCommitChunkFailureType(FailureType failureType) throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        // Index some docs
        indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var indexNodeIndicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode);

        // Ensure VBCC not yet uploaded
        assertNotNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        final var getChunkActionBlocked = new AtomicBoolean(false); // block only the manually triggered read action
        CountDownLatch actionAppeared = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")
                && getChunkActionBlocked.compareAndSet(false, true)) {
                actionAppeared.countDown();
                try {
                    if (failureType == FailureType.INDEX_CLOSED) {
                        assertBusy(() -> {
                            var s = indexNodeIndicesService.getShardOrNull(shardId);
                            assertNotNull(s);
                            assertThat(s.indexSettings().getIndexMetadata().getState(), equalTo(IndexMetadata.State.CLOSE));
                        });
                    } else {
                        // wait until blobs are deleted
                        assertBusy(() -> { assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty()); });
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Empty cache on search node, to ensure an action is sent to the indexing node
        evictSearchShardCache(indexName);

        var thread = new Thread(() -> {
            // serve Lucene files from the indexing node
            TestSearchType testSearchType = randomFrom(TestSearchType.values());
            if (failureType == FailureType.INDEX_CLOSED) {
                assertFailures(
                    prepareSearch(indexName, testSearchType),
                    Set.of(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.BAD_REQUEST),
                    containsString(IndexClosedException.class.getName())
                );
            } else {
                assertFailures(
                    prepareSearch(indexName, testSearchType),
                    Set.of(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE),
                    containsString(NoSuchFileException.class.getName())
                );
            }
        });
        thread.start();

        safeAwait(actionAppeared);
        switch (failureType) {
            case INDEX_DELETED:
                // Delete the index, which will make the action fail with blob not found (after it's deleted)
                assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
                break;
            case INDEX_CLOSED:
                // Close the index, which will make the action fail with IndexClosedException on the indexing node
                assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet());
                break;
            default:
                assert false : "unexpected failure type: " + failureType;
        }
        thread.join();
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWhenIndexIsDeleted() throws Exception {
        testGetVirtualBatchedCompoundCommitChunkFailureType(FailureType.INDEX_DELETED);
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWhenIndexCloses() throws Exception {
        testGetVirtualBatchedCompoundCommitChunkFailureType(FailureType.INDEX_CLOSED);
    }

    public void testGetVirtualBatchedCompoundCommitChunkRetryConnectivity() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        final var index = resolveIndex(indexName);
        final var indexShard = findIndexShard(index, 0);
        var shardId = indexShard.shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Ensure VBCC not yet uploaded
        assertNotNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        AtomicInteger counter = new AtomicInteger(0);
        final var transportService = MockTransportService.getInstance(indexNode);
        transportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                if (counter.getAndIncrement() <= 1) {
                    channel.sendResponse(
                        randomFrom(
                            new ConnectTransportException(transportService.getLocalNode(), "simulated"),
                            new CircuitBreakingException("Simulated", CircuitBreaker.Durability.TRANSIENT),
                            new NodeClosedException(transportService.getLocalNode()),
                            new ShardNotFoundException(indexShard.shardId())
                        )
                    );
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        // Empty cache on search node, to ensure an action is sent to the indexing node
        evictSearchShardCache(indexName);

        // serve Lucene files from the indexing node
        TestSearchType testSearchType = randomFrom(TestSearchType.values());
        validateSearchResponse(indexName, testSearchType, indexedDocs);

        // Upload VBCC
        flush(indexName);
        assertNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        // Search via the object store
        if (randomBoolean()) {
            evictSearchShardCache(indexName);
        }
        validateSearchResponse(indexName, testSearchType, indexedDocs);
    }

    public void testGetVirtualBatchedCompoundCommitChunkFileError() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        // Index some docs
        indexDocsAndRefresh(indexName);
        final var index = resolveIndex(indexName);
        final var indexShard = findIndexShard(index, 0);
        var shardId = indexShard.shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Ensure VBCC not yet uploaded
        assertNotNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        final var failedOnce = new AtomicBoolean(false); // fail only once
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]") && failedOnce.compareAndSet(false, true)) {
                GetVirtualBatchedCompoundCommitChunkRequest r = (GetVirtualBatchedCompoundCommitChunkRequest) request;
                GetVirtualBatchedCompoundCommitChunkRequest newRequest = new GetVirtualBatchedCompoundCommitChunkRequest(
                    r.getShardId(),
                    r.getPrimaryTerm(),
                    r.getVirtualBatchedCompoundCommitGeneration(),
                    Long.MAX_VALUE, // results in either a FileNotFoundException or NoSuchFileException from this test plugin
                    r.getLength(),
                    r.getPreferredNodeId()
                );
                connection.sendRequest(requestId, action, newRequest, options);
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });

        long primaryTermStart = findIndexShard(index, 0).getOperationPrimaryTerm();

        // Empty cache on search node, to ensure an action is sent to the indexing node
        evictSearchShardCache(indexName);

        // serve Lucene files from the indexing node
        TestSearchType testSearchType = randomFrom(TestSearchType.values());
        assertFailures(
            prepareSearch(indexName, testSearchType),
            Set.of(RestStatus.INTERNAL_SERVER_ERROR),
            either(containsString(FileNotFoundException.class.getName())).or(containsString(NoSuchFileException.class.getName()))
        );

        assertBusy(() -> {
            long primaryTermEnd = findIndexShard(indexName).getOperationPrimaryTerm();
            assertThat("failed shard should be re-allocated with new primary term", primaryTermEnd, greaterThan(primaryTermStart));
        });
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWithDifferentPrimaryTerm() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(indexShardCommitService.getCurrentVirtualBcc(shardId));

        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                GetVirtualBatchedCompoundCommitChunkRequest r = (GetVirtualBatchedCompoundCommitChunkRequest) request;
                GetVirtualBatchedCompoundCommitChunkRequest newRequest = new GetVirtualBatchedCompoundCommitChunkRequest(
                    r.getShardId(),
                    randomValueOtherThan(r.getPrimaryTerm(), ESTestCase::randomNonNegativeLong),
                    r.getVirtualBatchedCompoundCommitGeneration(),
                    r.getOffset(),
                    r.getLength(),
                    r.getPreferredNodeId()
                );
                connection.sendRequest(requestId, action, newRequest, options);
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });

        // Empty cache on search node, to ensure an action is sent to the indexing node
        evictSearchShardCache(indexName);

        // The different primary term throws a ResourceNotFoundException exception, so the cache blob reader switches to read from
        // blob store, where the BCC is not yet uploaded and thus not found.
        TestSearchType testSearchType = randomFrom(TestSearchType.values());
        assertFailures(
            prepareSearch(indexName, testSearchType),
            Set.of(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE),
            containsString(NoSuchFileException.class.getName())
        );
    }

}
