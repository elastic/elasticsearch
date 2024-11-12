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
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.store.Directory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import static co.elastic.elasticsearch.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure.CHUNK_REQUESTS_REJECTED_METRIC;
import static co.elastic.elasticsearch.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure.CURRENT_CHUNKS_BYTES_METRIC;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.BCC_ELAPSED_TIME_BEFORE_FREEZE_HISTOGRAM_METRIC;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.BCC_NUMBER_COMMITS_HISTOGRAM_METRIC;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.BCC_TOTAL_SIZE_HISTOGRAM_METRIC;
import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.action.search.SearchTransportService.QUERY_ACTION_NAME;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
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

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                sharedBlobCacheWarmingService,
                refreshThrottlerFactory,
                statelessCommitService.getIndexEngineLocalReaderListenerForShard(engineConfig.getShardId()),
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics
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
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                client,
                commitCleaner,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            return new NoopSharedBlobCacheWarmingService(cacheService, threadPool, settings);
        }
    }

    public static class TestStatelessCommitService extends StatelessCommitService {

        private final AtomicReference<Consumer<VirtualBatchedCompoundCommit>> uploadingVbccConsumerRef = new AtomicReference<>();

        public TestStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            super(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService, telemetryProvider);
        }

        @Override
        protected ShardCommitState createShardCommitState(
            ShardId shardId,
            long primaryTerm,
            BooleanSupplier inititalizingNoSearchSupplier,
            TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
            Runnable triggerTranslogReplicator
        ) {
            return new ShardCommitState(
                shardId,
                primaryTerm,
                inititalizingNoSearchSupplier,
                addGlobalCheckpointListenerFunction,
                triggerTranslogReplicator
            ) {
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
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    private void evictSearchShardCache(String indexName) {
        IndexShard shard = findSearchShard(indexName);
        Directory directory = shard.store().directory();
        BlobStoreCacheDirectory blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(directory);
        getCacheService(blobStoreCacheDirectory).forceEvict((key) -> true);
    }

    private record IndexedDocs(int numDocs, int customDocs, long sum) {}

    /**
     * Indexes a number of documents until the directory has a size of at least a few pages. The mappings are:
     * - "field" with a random string of length between 1 and 25
     * - "number" with a random long between 0 and the current index
     * - "custom" with a value "value" for customDocs number of docs. customDocs will be less or equal to numDocs.
     *
     * @return a {@link IndexedDocs} object with the number of docs, the number of custom docs and the sum of the numbers.
     */
    private IndexedDocs indexDocs(String indexName) throws Exception {
        int totalDocs = 0;
        int totalCustomDocs = 0;
        long sum = 0;
        long pages = randomLongBetween(6, 8);

        assertThat("this function only works for 1 shard indices", getNumShards(indexName).numPrimaries, equalTo(1));
        var shard = findIndexShard(indexName);
        var directory = shard.store().directory();

        // Disable scheduled refresh and manually perform local refresh to get expected directory size
        final TimeValue originalRefreshInterval = shard.indexSettings().getRefreshInterval();
        updateIndexSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1), indexName);
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
                            randomUnicodeOfCodepointLengthBetween(50, 100),
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
            shard.refresh("update directory size");
        } while (getDirectorySize(directory) <= PAGE_SIZE * pages);
        updateIndexSettings(
            Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), originalRefreshInterval),
            indexName
        );

        logger.info("--> indexed {} docs in {} with {} custom docs, sum {}", totalDocs, indexName, totalCustomDocs, sum);
        return new IndexedDocs(totalDocs, totalCustomDocs, sum);
    }

    private IndexedDocs indexDocsAndRefresh(String indexName) throws Exception {
        final var indexedDocs = indexDocs(indexName);
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        logger.info("--> directory size {}", getDirectorySize(findIndexShard(indexName).store().directory()));
        return indexedDocs;
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
        // Set a large size to retrieve all documents to force reading all relevant search data
        return switch (testSearchType) {
            case MATCH_ALL -> prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(10_000);
            case MATCH_CUSTOM -> prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")).setSize(10_000);
            case SUM -> prepareSearch(indexName).addAggregation(sum("sum").field("number")).setSize(10_000);
        };
    }

    static Consumer<SearchResponse> getSearchResponseValidator(TestSearchType testSearchType, IndexedDocs indexedDocs) {
        // We avoid setSize(0) with the below statements, because we want the searches to fetch some chunks.
        return switch (testSearchType) {
            case MATCH_ALL -> searchResponse -> assertEquals(indexedDocs.numDocs, searchResponse.getHits().getTotalHits().value());
            case MATCH_CUSTOM -> searchResponse -> assertEquals(indexedDocs.customDocs, searchResponse.getHits().getTotalHits().value());
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
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var vbccGen = statelessCommitService.getCurrentVirtualBcc(shardId).getPrimaryTermAndGeneration().generation();

        // serve Lucene files from the indexing node
        if (randomBoolean()) {
            evictSearchShardCache(indexName);
        }

        TestSearchType testSearchType = randomFrom(TestSearchType.values());
        validateSearchResponse(indexName, testSearchType, indexedDocs);

        // Ensure VBCC not yet uploaded
        assertThat(statelessCommitService.getCurrentVirtualBcc(shardId).getPrimaryTermAndGeneration().generation(), equalTo(vbccGen));

        // Upload VBCC
        flush(indexName);
        assertNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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

        var threads = new Thread[randomIntBetween(1, 8)];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                // serve Lucene files from the indexing node
                TestSearchType testSearchType = randomFrom(TestSearchType.values());
                validateSearchResponse(indexName, testSearchType, indexedDocs);
            });
            threads[i].start();
        }

        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testGetVirtualBatchedCompoundCommitIfPrimaryRelocates() throws Exception {
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
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

        CountDownLatch actionAppeared = new CountDownLatch(1);
        CountDownLatch getVBCCChunkBlocked = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (connection.getNode().getName().equals(indexNodeA)
                && action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                actionAppeared.countDown();
                safeAwait(getVBCCChunkBlocked);
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

        CountDownLatch indexNotFoundOnIndexNodeA = new CountDownLatch(1);
        final var indexNodeATransportService = MockTransportService.getInstance(indexNodeA);
        indexNodeATransportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                @Override
                public void sendResponse(Exception exception) {
                    indexNotFoundOnIndexNodeA.countDown();
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
        indexNodeBTransportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                assert false : "Search shard should not send any requests to the indexNodeB and should have fetched from object store";
            }
        );

        getVBCCChunkBlocked.countDown();
        safeAwait(indexNotFoundOnIndexNodeA);
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
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var indexNodeIndicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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
                            new EsRejectedExecutionException("simulated")
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
        assertNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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
        indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

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

    public void testGetVirtualBatchedCompoundCommitDoNotDeadlockWaitingForResponses() throws Exception {
        int nodeProcessors = 2;
        // SHARD_READ_THREAD_POOL with 2 processors has 8 threads, we need at least 8 searches
        // waiting for a cache chunk to force the deadlock
        int searchThreadPoolSize = nodeProcessors * 4;
        var nodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofMb(16))
            .put(NODE_PROCESSORS_SETTING.getKey(), nodeProcessors)
            .put("thread_pool.search.size", searchThreadPoolSize)
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4))
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        startSearchNode(nodeSettings);

        List<String> indexNames = new ArrayList<>();
        for (int i = 0; i < searchThreadPoolSize; i++) {
            var indexName = randomIdentifier();
            createIndex(indexName, 1, 1);
            ensureGreen(indexName);
            indexNames.add(indexName);
            indexDocsAndRefresh(indexName, randomIntBetween(50, 100));
        }

        Queue<CheckedRunnable<Exception>> delayedReadChunks = new LinkedBlockingQueue<>();
        CountDownLatch readChunkRequestsReceived = new CountDownLatch(searchThreadPoolSize);
        AtomicBoolean delayReadChunks = new AtomicBoolean(true);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    if (delayReadChunks.get()) {
                        delayedReadChunks.add(() -> handler.messageReceived(request, channel, task));
                        readChunkRequestsReceived.countDown();
                    } else {
                        handler.messageReceived(request, channel, task);
                    }
                }
            );

        logger.info("--> evict search shard cache");
        for (String indexName : indexNames) {
            evictSearchShardCache(indexName);
        }

        logger.info("--> trigger searches for 8 indices");
        var searchFutures = new ArrayList<ActionFuture<SearchResponse>>();
        for (String indexName : indexNames) {
            searchFutures.add(client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).execute());
        }

        safeAwait(readChunkRequestsReceived);
        delayReadChunks.set(false);
        CheckedRunnable<Exception> delayedReadChunk;
        while ((delayedReadChunk = delayedReadChunks.poll()) != null) {
            delayedReadChunk.run();
        }
        for (ActionFuture<SearchResponse> searchFuture : searchFutures) {
            assertNoFailuresAndResponse(searchFuture, response -> {});
        }
    }

    public void testGetVirtualBatchedCompoundCommitShouldNotStuckForConcurrentShardClose() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        final var indexNodeB = startIndexNode();
        ensureStableCluster(4);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);

        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

        CountDownLatch getVBCCChunkSent = new CountDownLatch(1);
        CountDownLatch getVBCCChunkBlocked = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (connection.getNode().getName().equals(indexNodeA)
                && action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                getVBCCChunkSent.countDown();
                safeAwait(getVBCCChunkBlocked);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);

        // Wait till search shard start recovery
        safeAwait(getVBCCChunkSent);

        if (randomBoolean()) {
            internalCluster().stopNode(indexNodeA);
        } else {
            // Relocate primary, which will make original indexing node throw IndexNotFoundException for GetVBCC action
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
            final String indexNodeBNodeId = getNodeId(indexNodeB);
            awaitClusterState(
                logger,
                indexNodeA,
                clusterState -> clusterState.routingTable()
                    .index(indexName)
                    .shard(shardId.id())
                    .primaryShard()
                    .currentNodeId()
                    .equals(indexNodeBNodeId)
            );
            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
            logger.info("--> relocated primary");
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
        }

        getVBCCChunkBlocked.countDown();
        // The first recovery for the search shard will fail but it will recover quickly again with the new primary
        ensureGreen(indexName);
    }

    public void testSearchKeepsRunningWhenSearchShardRelocates() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        var searchNodeA = startSearchNode();
        var searchNodeB = startSearchNode();
        ensureStableCluster(4);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", searchNodeB).build());
        ensureGreen(indexName);

        // Index some docs
        final IndexedDocs indexedDocs = indexDocsAndRefresh(indexName);
        var shardId = findIndexShard(indexName).shardId();
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        // Ensure VBCC not yet uploaded
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

        // Empty cache on search node, to ensure an action is sent to the indexing node
        evictSearchShardCache(indexName);

        final var getVBCCChunkSent = new CountDownLatch(1);
        final var getVBCCChunkBlocked = new CountDownLatch(1);
        final var searchNodeATransportService = MockTransportService.getInstance(searchNodeA);
        searchNodeATransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (connection.getNode().getName().equals(indexNode)
                && action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                getVBCCChunkSent.countDown();
                safeAwait(getVBCCChunkBlocked);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final CountDownLatch searchCompletionLatch = new CountDownLatch(1);
        final Thread searchThread = new Thread(() -> {
            validateSearchResponse(indexName, TestSearchType.MATCH_ALL, indexedDocs);
            searchCompletionLatch.countDown();
        });
        logger.info("--> starting search");
        searchThread.start();

        // Wait till search shard start searching
        safeAwait(getVBCCChunkSent);

        // Relocate the search shard
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeA))));
        logger.info("--> relocated search shard");

        getVBCCChunkBlocked.countDown();
        ensureGreen(indexName);
        safeAwait(searchCompletionLatch);
        searchThread.join();
    }

    public void testSearchEngineDoesNotFailOnCircuitBreakerExceptionsDuringIndexing() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        AtomicInteger exceptionsCounter = new AtomicInteger();
        int maxAttempts = randomIntBetween(1, 8);
        var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                if (exceptionsCounter.getAndIncrement() <= maxAttempts) {
                    channel.sendResponse(new CircuitBreakingException("Simulated", CircuitBreaker.Durability.TRANSIENT));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        // We should retry indefinitely on CircuitBreakingException during indexing, eventually make progress and
        // don't fail the search shard
        indexDocsAndRefresh(indexName);
        ensureSearchable(indexName);
    }

    public void testVirtualBatchedCompoundCommitChunksPressure() {
        // The test admits a first refresh that requests a 1-page chunk, and halts it mid-way before returning the chunk response.
        // Then, a second refresh comes in, that requests another 1-page chunk. It is rejected two times in a row, and the third retry
        // attempt is halted mid-way before processing the chunk request (and thus is not yet counted by the pressure). Then, we complete
        // the first refresh, which resets the pressure, and allow the second refresh to complete successfully.

        startMasterOnlyNode();
        final var indexNode = startIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(GetVirtualBatchedCompoundCommitChunksPressure.CHUNKS_BYTES_LIMIT.getKey(), (PAGE_SIZE + 1) + "b")
                // We need at least 2 vbcc chunk threads to be able to reject the second refresh requests.
                .put(NODE_PROCESSORS_SETTING.getKey(), 2)
                .build()
        );
        startSearchNode(
            Settings.builder()
                // We need at least 2 refresh threads to be able to reject the second refresh requests.
                .put(NODE_PROCESSORS_SETTING.getKey(), 2)
                // We need enough free regions so that the search node is able to request pages/regions when refreshing an index.
                .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(PAGE_SIZE))
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(32 * PAGE_SIZE))
                .build()
        );
        final var indexSettings = indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            .build();
        final String indexName1 = randomIdentifier();
        createIndex(indexName1, indexSettings);
        final String indexName2 = randomIdentifier();
        createIndex(indexName2, indexSettings);
        ensureGreen(indexName1, indexName2);
        final var index1shardId = findIndexShard(indexName1).shardId();
        final var index2shardId = findIndexShard(indexName2).shardId();
        final TestTelemetryPlugin metricsPlugin = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        metricsPlugin.resetMeter();
        final var vbccChunksPressure = internalCluster().getInstance(GetVirtualBatchedCompoundCommitChunksPressure.class, indexNode);

        // Index only 1 doc for each index
        indexDoc(indexName1, "1", "f", "v");
        indexDoc(indexName2, "1", "f", "v");

        // serve Lucene files from the indexing node
        evictSearchShardCache(indexName1);
        evictSearchShardCache(indexName2);

        // Infrastructure to be able to catch the chunks of the refreshes mid-way.
        AtomicInteger pagesRead = new AtomicInteger(0);
        CountDownLatch chunk1ResponseProduced = new CountDownLatch(1); // chunk of first refresh counted by pressure, and halted mid-way
        CountDownLatch chunk1ToSendResponse = new CountDownLatch(1); // to send the response for the first refresh and release the pressure
        CountDownLatch chunk2Attempts = new CountDownLatch(3); // to count the chunk requests of the second refresh before halting
        CountDownLatch chunk2ToProcess = new CountDownLatch(1); // to halt before processing the third request of the second refresh
        final var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                var r = (GetVirtualBatchedCompoundCommitChunkRequest) request;
                if (r.getShardId().equals(index1shardId)) {
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }

                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            chunk1ResponseProduced.countDown();
                            safeAwait(chunk1ToSendResponse);
                            channel.sendResponse(response);
                            pagesRead.incrementAndGet();
                        }
                    }, task);
                } else if (r.getShardId().equals(index2shardId)) {
                    chunk2Attempts.countDown();
                    if (chunk2Attempts.getCount() == 0) {
                        // halt the third attempt of the second refresh
                        safeAwait(chunk2ToProcess);
                    }
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public void sendResponse(Exception exception) {
                            assertThat(chunk2Attempts.getCount(), greaterThan(0L));
                            final var rejectedException = ExceptionsHelper.unwrap(exception, EsRejectedExecutionException.class);
                            assertNotNull(rejectedException);
                            assertThat(
                                rejectedException.getMessage(),
                                containsString(
                                    "rejected execution of VBCC chunk request [current_chunks_bytes=4096, "
                                        + "request=4096, chunks_bytes_limit=4097]"
                                )
                            );
                            channel.sendResponse(exception);
                        }

                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            assertThat(chunk2Attempts.getCount(), equalTo(0L));
                            channel.sendResponse(response);
                        }
                    }, task);
                } else {
                    assert false : "unexpected shard id: " + r.getShardId();
                }
            }
        );

        // Refresh first index
        var refresh1 = client().admin().indices().prepareRefresh(indexName1).execute();
        safeAwait(chunk1ResponseProduced);
        logger.info("--> chunk produced for the first refresh");
        assertThat(vbccChunksPressure.getCurrentChunksBytes(), equalTo((long) PAGE_SIZE));

        logger.info("--> issuing second refresh");
        var refresh2 = client().admin().indices().prepareRefresh(indexName2).execute();

        // wait until the third attempt of the second refresh is halted
        safeAwait(chunk2Attempts);

        logger.info("--> continuing sending chunk for the first refresh");
        chunk1ToSendResponse.countDown();
        assertNoFailures(safeGet(refresh1));
        assertThat(vbccChunksPressure.getCurrentChunksBytes(), equalTo(0L));

        logger.info("--> continuing processing chunk for the second refresh");
        chunk2ToProcess.countDown();
        assertNoFailures(safeGet(refresh2));
        assertThat(vbccChunksPressure.getCurrentChunksBytes(), equalTo(0L));

        // Confirm that the pressure metrics were correctly set
        final int pages = pagesRead.get();
        var measurements = metricsPlugin.getLongUpDownCounterMeasurement(CURRENT_CHUNKS_BYTES_METRIC);
        assertThat(measurements.size(), equalTo(pages * 4));
        for (int p = 0; p < pages; p++) {
            // The first refresh results in two measurements (one that adds bytes, and one that removes bytes) for each page chunk request
            assertMeasurement(measurements.get(p * 2), PAGE_SIZE, indexName1, index1shardId);
            assertMeasurement(measurements.get(p * 2 + 1), -PAGE_SIZE, indexName1, index1shardId);
            // The second refresh had the same amount of measurements, that appear after the first refresh's measurements
            assertMeasurement(measurements.get(pages * 2 + p * 2), PAGE_SIZE, indexName2, index2shardId);
            assertMeasurement(measurements.get(pages * 2 + p * 2 + 1), -PAGE_SIZE, indexName2, index2shardId);
        }

        measurements = metricsPlugin.getLongCounterMeasurement(CHUNK_REQUESTS_REJECTED_METRIC);
        assertThat(measurements.size(), equalTo(2));
        assertRejectionMeasurement(measurements.get(0), PAGE_SIZE, indexName2, index2shardId);
        assertRejectionMeasurement(measurements.get(1), PAGE_SIZE, indexName2, index2shardId);
    }

    public void testVirtualBatchedCompoundCommitUploadMetrics() throws Exception {
        final var indexNode = startMasterAndIndexNode(
            Settings.builder().put(disableIndexingDiskAndMemoryControllersNodeSettings()).build()
        );

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());

        var shardId = findIndexShard(indexName).shardId();
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        final var metricsPlugin = findPlugin(indexNode, TestTelemetryPlugin.class);
        metricsPlugin.resetMeter();

        final int numberCommits = between(5, 8);
        for (int i = 0; i < numberCommits; i++) {
            indexDocsAndRefresh(indexName);
            if (randomBoolean() || i == numberCommits - 1) {
                final VirtualBatchedCompoundCommit virtualBcc = statelessCommitService.getCurrentVirtualBcc(shardId);
                final long minAge = threadPool.relativeTimeInMillis() - virtualBcc.getCreationTimeInMillis();
                // Trigger upload
                flush(indexName);

                final List<Measurement> sizeMeasurements = metricsPlugin.getLongHistogramMeasurement(BCC_TOTAL_SIZE_HISTOGRAM_METRIC);
                assertThat(sizeMeasurements, hasSize(1));
                assertMeasurement2(sizeMeasurements.get(0), ByteSizeUnit.BYTES.toMB(virtualBcc.getTotalSizeInBytes()), indexName, shardId);

                final List<Measurement> nCommitsMeasurements = metricsPlugin.getLongHistogramMeasurement(
                    BCC_NUMBER_COMMITS_HISTOGRAM_METRIC
                );
                assertThat(nCommitsMeasurements, hasSize(1));
                assertMeasurement2(nCommitsMeasurements.get(0), virtualBcc.size(), indexName, shardId);

                final List<Measurement> ageMeasurements = metricsPlugin.getLongHistogramMeasurement(
                    BCC_ELAPSED_TIME_BEFORE_FREEZE_HISTOGRAM_METRIC
                );
                assertThat(ageMeasurements, hasSize(1));
                assertThat(ageMeasurements.get(0).attributes(), equalTo(Map.of("index_name", indexName, "shard_id", shardId.id())));
                // The exact value of age is not important as long as it is greater or equal than the minimum age
                // that is measured before creating the upload task
                assertThat(ageMeasurements.get(0).getLong(), greaterThanOrEqualTo(minAge));
                metricsPlugin.resetMeter();
            }
        }
    }

    // TODO: merge the following two methods once we change all attribute names to be snake_case
    private void assertMeasurement(Measurement measurement, long value, String indexName, ShardId shardId) {
        assertThat(measurement.getLong(), equalTo(value));
        assertThat(measurement.attributes().get("index_name"), equalTo(indexName));
        assertThat(measurement.attributes().get("shard_id"), equalTo(shardId.id()));
    }

    private void assertMeasurement2(Measurement measurement, long value, String indexName, ShardId shardId) {
        assertThat(measurement.getLong(), equalTo(value));
        assertThat(measurement.attributes().get("index_name"), equalTo(indexName));
        assertThat(measurement.attributes().get("shard_id"), equalTo(shardId.id()));
    }

    private void assertRejectionMeasurement(Measurement measurement, int bytes, String indexName, ShardId shardId) {
        assertMeasurement(measurement, 1L, indexName, shardId);
        assertThat(measurement.attributes().get("rejected_bytes"), equalTo(bytes));
    }
}
