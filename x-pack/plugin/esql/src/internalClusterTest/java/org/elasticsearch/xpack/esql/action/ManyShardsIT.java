/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Make sures that we can run many concurrent requests with large number of shards with any data_partitioning.
 */
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class ManyShardsIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        var plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MockSearchService.TestPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalExchangePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 5000)))
            .build();
    }

    @Before
    public void setupIndices() {
        int numIndices = between(10, 20);
        for (int i = 0; i < numIndices; i++) {
            String manyType = randomFrom("date", "date_nanos", "keyword", "long");
            String index = "test-" + i + "__" + manyType;
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.shard.check_on_startup", "false")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("user", "type=keyword", "tags", "type=keyword", "many_type", "type=" + manyType, "single_type", "type=long")
                .get();
            BulkRequestBuilder bulk = client().prepareBulk(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int numDocs = between(10, 25); // every shard has at least 1 doc
            long startInMillis = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-10-23T13:52:55.015Z");
            for (int d = 0; d < numDocs; d++) {
                String user = randomFrom("u1", "u2", "u3");
                String tag = randomFrom("java", "elasticsearch", "lucene");
                long millis = startInMillis + randomLongBetween(0, TimeValue.timeValueDays(1).millis());
                String many = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(millis);
                Map<String, Object> source = Map.of("user", user, "tags", tag, "single_type", millis, "many_type", many);
                bulk.add(client().prepareIndex().setSource(source));
            }
            bulk.get();
        }
    }

    public void testConcurrentQueries() throws Exception {
        int numQueries = between(10, 20);
        Thread[] threads = new Thread[numQueries];
        CountDownLatch latch = new CountDownLatch(1);
        for (int q = 0; q < numQueries; q++) {
            threads[q] = new Thread(() -> {
                safeAwait(latch);
                final var pragmas = Settings.builder();
                if (randomBoolean() && canUseQueryPragmas()) {
                    pragmas.put(randomPragmas().getSettings())
                        .put("task_concurrency", between(1, 2))
                        .put("exchange_concurrent_clients", between(1, 2));
                }
                try (
                    var response = run(
                        syncEsqlQueryRequest("from test-* | stats count(user) by tags").pragmas(new QueryPragmas(pragmas.build()))
                    )
                ) {
                    // do nothing
                } catch (Exception | AssertionError e) {
                    logger.warn("Query failed with exception", e);
                    throw e;
                }
            }, "testConcurrentQueries-" + q);
        }
        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join(10_000);
        }
    }

    public void testRejection() throws Exception {
        DiscoveryNode dataNode = randomFrom(internalCluster().clusterService().state().nodes().getDataNodes().values());
        String indexName = "single-node-index";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.require._name", dataNode.getName())
            )
            .setMapping("user", "type=keyword", "tags", "type=keyword")
            .get();
        client().prepareIndex(indexName)
            .setSource("user", "u1", "tags", "lucene")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        MockTransportService ts = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getName());
        CountDownLatch dataNodeRequestLatch = new CountDownLatch(1);
        ts.addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
            handler.messageReceived(request, channel, task);
            dataNodeRequestLatch.countDown();
        });

        ts.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, (handler, request, channel, task) -> {
            ts.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    channel.sendResponse(e);
                }

                @Override
                protected void doRun() throws Exception {
                    assertTrue(dataNodeRequestLatch.await(30, TimeUnit.SECONDS));
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            channel.sendResponse(new RemoteTransportException("simulated", new EsRejectedExecutionException("test queue")));
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }
                    }, task);
                }
            });
        });

        try {
            AtomicReference<Exception> failure = new AtomicReference<>();
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("from single-node-index | stats count(user) by tags");
            request.acceptedPragmaRisks(true);
            request.pragmas(randomPragmas());
            CountDownLatch queryLatch = new CountDownLatch(1);
            client().execute(EsqlQueryAction.INSTANCE, request, ActionListener.runAfter(ActionListener.wrap(r -> {
                r.close();
                throw new AssertionError("expected failure");
            }, failure::set), queryLatch::countDown));
            assertTrue(queryLatch.await(10, TimeUnit.SECONDS));
            assertThat(failure.get(), instanceOf(EsRejectedExecutionException.class));
            assertThat(ExceptionsHelper.status(failure.get()), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(failure.get().getMessage(), equalTo("test queue"));
        } finally {
            ts.clearAllRules();
        }
    }

    static class SearchContextCounter {
        private final int maxAllowed;
        private final AtomicInteger current = new AtomicInteger();

        SearchContextCounter(int maxAllowed) {
            this.maxAllowed = maxAllowed;
        }

        void onNewContext() {
            int total = current.incrementAndGet();
            assertThat("opening more shards than the limit", total, Matchers.lessThanOrEqualTo(maxAllowed));
        }

        void onContextReleased() {
            int total = current.decrementAndGet();
            assertThat(total, Matchers.greaterThanOrEqualTo(0));
        }
    }

    public void testLimitConcurrentShards() {
        Iterable<SearchService> searchServices = internalCluster().getInstances(SearchService.class);
        try {
            var queries = List.of(
                "from test-* | stats count(user) by tags",
                "from test-* | stats count(user) by tags | LIMIT 0",
                "from test-* | stats count(user) by tags | LIMIT 1",
                "from test-* | stats count(user) by tags | LIMIT 1000",
                "from test-* | LIMIT 0",
                "from test-* | LIMIT 1",
                "from test-* | LIMIT 1000",
                "from test-* | SORT tags | LIMIT 0",
                "from test-* | SORT tags | LIMIT 1",
                "from test-* | SORT tags | LIMIT 1000"
            );
            for (String q : queries) {
                var pragmas = randomPragmas();
                // For queries involving TopN, the node-reduce driver may hold on to contexts for longer (due to late materialization, which
                // is only turned when the NODE_LEVEL_REDUCTION is turned on), so we don't check against the limit.
                boolean nodeLevelReduction = QueryPragmas.NODE_LEVEL_REDUCTION.get(pragmas.getSettings());
                int maxAllowed = q.contains("SORT tags") && nodeLevelReduction ? Integer.MAX_VALUE : pragmas.maxConcurrentShardsPerNode();
                for (SearchService searchService : searchServices) {
                    SearchContextCounter counter = new SearchContextCounter(maxAllowed);
                    var mockSearchService = (MockSearchService) searchService;
                    mockSearchService.setOnCreateSearchContext(r -> counter.onNewContext());
                    mockSearchService.setOnRemoveContext(r -> counter.onContextReleased());
                }
                run(syncEsqlQueryRequest(q).pragmas(pragmas)).close();
            }
        } finally {
            for (SearchService searchService : searchServices) {
                var mockSearchService = (MockSearchService) searchService;
                mockSearchService.setOnCreateSearchContext(r -> {});
                mockSearchService.setOnRemoveContext(r -> {});
            }
        }
    }

    public void testCancelUnnecessaryRequests() {
        assumeTrue("Requires pragmas", canUseQueryPragmas());
        internalCluster().ensureAtLeastNumDataNodes(3);

        var coordinatingNode = internalCluster().getNodeNames()[0];

        var exchanges = new AtomicInteger(0);
        var coordinatorNodeTransport = MockTransportService.getInstance(coordinatingNode);
        coordinatorNodeTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (Objects.equals(action, ExchangeService.OPEN_EXCHANGE_ACTION_NAME)) {
                logger.info("Opening exchange on node [{}]", connection.getNode().getId());
                exchanges.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var query = syncEsqlQueryRequest("from test-* | LIMIT 1").pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1).build())
        );

        try (var result = safeGet(client().execute(EsqlQueryAction.INSTANCE, query))) {
            assertThat(Iterables.size(result.rows()), equalTo(1L));
            assertThat(exchanges.get(), lessThanOrEqualTo(2));
        } finally {
            coordinatorNodeTransport.clearAllRules();
        }
    }

    private static final String ONE_SHARD_PER_NODE_INDEX = "one-shard-per-node";

    /**
     * With one node queried at a time and a {@code LIMIT} satisfied by the first node, the remaining nodes must be skipped
     * even if the coordinator's final driver has not consumed the rows yet. See {@link #queryWithStarvedCoordinator}.
     */
    public void testSkipNodesWhenReceivedRowsSatisfyLimit() throws Exception {
        assumeTrue("Requires pragmas", canUseQueryPragmas());
        int limit = between(1, Math.toIntExact(createOneShardPerNodeIndex().minDocsPerShard()));
        int queriedNodes = queryWithStarvedCoordinator(
            "FROM " + ONE_SHARD_PER_NODE_INDEX + " | LIMIT " + limit,
            result -> assertThat(Iterables.size(result.rows()), equalTo((long) limit))
        );
        assertThat(queriedNodes, equalTo(1));
    }

    /**
     * A {@code LIMIT} that no single node can satisfy must still return all the rows it asks for, so no node may be skipped.
     */
    public void testQueryAllNodesWhenNoNodeSatisfiesLimit() throws Exception {
        assumeTrue("Requires pragmas", canUseQueryPragmas());
        int numDocs = createOneShardPerNodeIndex().numDocs();
        int queriedNodes = queryWithStarvedCoordinator(
            "FROM " + ONE_SHARD_PER_NODE_INDEX + " | LIMIT " + numDocs,
            result -> assertThat(Iterables.size(result.rows()), equalTo((long) numDocs))
        );
        assertThat(queriedNodes, equalTo(internalCluster().numDataNodes()));
    }

    /**
     * Without an explicit {@code LIMIT} the default result limit applies, which is larger than the whole index here.
     */
    public void testQueryAllNodesWithoutLimit() throws Exception {
        assumeTrue("Requires pragmas", canUseQueryPragmas());
        int numDocs = createOneShardPerNodeIndex().numDocs();
        assertThat(numDocs, lessThan(AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY)));
        int queriedNodes = queryWithStarvedCoordinator(
            "FROM " + ONE_SHARD_PER_NODE_INDEX,
            result -> assertThat(Iterables.size(result.rows()), equalTo((long) numDocs))
        );
        assertThat(queriedNodes, equalTo(internalCluster().numDataNodes()));
    }

    /**
     * The coordinator aggregates the rows it receives, so no {@code LIMIT} reads the exchange directly and every node is needed.
     */
    public void testQueryAllNodesForStats() throws Exception {
        assumeTrue("Requires pragmas", canUseQueryPragmas());
        int numDocs = createOneShardPerNodeIndex().numDocs();
        int queriedNodes = queryWithStarvedCoordinator(
            "FROM " + ONE_SHARD_PER_NODE_INDEX + " | STATS c = COUNT(*)",
            result -> assertThat(EsqlTestUtils.getValuesList(result), equalTo(List.of(List.of((long) numDocs))))
        );
        assertThat(queriedNodes, equalTo(internalCluster().numDataNodes()));
    }

    /**
     * Any node may hold the rows that sort first, so a {@code SORT} before the {@code LIMIT} needs every node, even though
     * the first node alone returns enough rows.
     */
    public void testQueryAllNodesForSortedLimit() throws Exception {
        assumeTrue("Requires pragmas", canUseQueryPragmas());
        OneShardPerNodeIndex index = createOneShardPerNodeIndex();
        int limit = between(1, Math.toIntExact(index.minDocsPerShard()));
        List<List<Object>> expected = IntStream.range(0, index.numDocs())
            .mapToObj(d -> "u" + d)
            .sorted()
            .limit(limit)
            .map(user -> List.<Object>of(user))
            .toList();
        int queriedNodes = queryWithStarvedCoordinator(
            "FROM " + ONE_SHARD_PER_NODE_INDEX + " | SORT user | LIMIT " + limit,
            result -> assertThat(EsqlTestUtils.getValuesList(result), equalTo(expected))
        );
        assertThat(queriedNodes, equalTo(internalCluster().numDataNodes()));
    }

    private record OneShardPerNodeIndex(int numDocs, long minDocsPerShard) {}

    /**
     * Creates an index with exactly one shard on each of at least three data nodes. A node holding a single shard completes
     * without waiting for the coordinator to drain its pages, so the coordinator cannot rely on backpressure to stop early.
     */
    private OneShardPerNodeIndex createOneShardPerNodeIndex() {
        internalCluster().ensureAtLeastNumDataNodes(3);
        int numDocs = between(50, 100) * internalCluster().numDataNodes();
        client().admin()
            .indices()
            .prepareCreate(ONE_SHARD_PER_NODE_INDEX)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, internalCluster().numDataNodes())
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.total_shards_per_node", 1)
            )
            .setMapping("user", "type=keyword")
            .get();
        BulkRequestBuilder bulk = client().prepareBulk(ONE_SHARD_PER_NODE_INDEX).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int d = 0; d < numDocs; d++) {
            bulk.add(client().prepareIndex().setSource("user", "u" + d));
        }
        bulk.get();
        ensureGreen(ONE_SHARD_PER_NODE_INDEX);
        long minDocsPerShard = Long.MAX_VALUE;
        for (var shardStats : client().admin().indices().prepareStats(ONE_SHARD_PER_NODE_INDEX).clear().setDocs(true).get().getShards()) {
            long docs = shardStats.getStats().getDocs().getCount();
            assertThat("no doc for shard " + shardStats.getShardRouting().shardId(), docs, greaterThan(0L));
            minDocsPerShard = Math.min(minDocsPerShard, docs);
        }
        return new OneShardPerNodeIndex(numDocs, minDocsPerShard);
    }

    /**
     * Runs {@code esqlQuery} from a new coordinating-only node, querying one data node at a time. Once the first exchange is
     * opened, the coordinator's ES|QL worker threads are kept busy until a second exchange is opened, so its final driver cannot
     * consume any rows before the next node is picked. The coordinator holds no shards, so every exchange it opens goes through
     * the mock transport and is counted.
     *
     * @return the number of data nodes the coordinator queried
     */
    private int queryWithStarvedCoordinator(String esqlQuery, Consumer<EsqlQueryResponse> checkResult) throws Exception {
        String coordinatingNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        var exchanges = new AtomicInteger();
        var releaseWorkers = new CountDownLatch(1);
        var coordinatorTransport = MockTransportService.getInstance(coordinatingNode);
        coordinatorTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(ExchangeService.OPEN_EXCHANGE_ACTION_NAME)) {
                if (exchanges.incrementAndGet() == 1) {
                    blockWorkers(coordinatorTransport.getThreadPool(), releaseWorkers);
                } else {
                    releaseWorkers.countDown();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var query = syncEsqlQueryRequest(esqlQuery).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1).build())
        );
        try (var result = safeGet(client(coordinatingNode).execute(EsqlQueryAction.INSTANCE, query))) {
            checkResult.accept(result);
            return exchanges.get();
        } finally {
            releaseWorkers.countDown();
            coordinatorTransport.clearAllRules();
            internalCluster().stopNode(coordinatingNode);
        }
    }

    /**
     * Occupies every ES|QL worker thread so that no driver can run until {@code release} is counted down or a few seconds pass,
     * which is far longer than the coordinator takes to decide whether to query the next node.
     */
    private static void blockWorkers(ThreadPool threadPool, CountDownLatch release) {
        int workers = threadPool.info(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME).getMax();
        for (int i = 0; i < workers; i++) {
            threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME).execute(() -> {
                try {
                    release.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    public void testMultiTypes() {
        String q0 = """
            FROM test-* METADATA _index
            | EVAL many_date = TO_DATETIME(many_type), many_str = TO_STRING(many_type), many_long = TO_LONG(many_type)
            | KEEP _index, single_type, many_date, many_str, many_long
            """;
        String q1 = q0 + " | SORT _index";
        String q2 = q0 + " | SORT single_type";
        String q3 = q0 + " | SORT single_type, _index";
        for (String q : List.of(q0, q1, q2, q3)) {
            logger.debug("--> running query:\n{}", q);
            try (var resp = run(q)) {
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                for (List<Object> row : rows) {
                    String index = (String) row.get(0);
                    String manyType = index.substring(index.indexOf("__") + 2);
                    long singleValue = (long) row.get(1);
                    String manyDate = (String) row.get(2);
                    assertThat(manyDate, equalTo(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(singleValue)));
                    switch (manyType) {
                        case "keyword" -> {
                            String manyKeyword = (String) row.get(3);
                            assertThat(manyKeyword, equalTo(manyDate));
                            assertNull(row.get(4));
                        }
                        case "long" -> {
                            String manyKeyword = (String) row.get(3);
                            assertThat(manyKeyword, equalTo(Long.toString(singleValue)));
                            long manyLong = (long) row.get(4);
                            assertThat(manyLong, equalTo(singleValue));
                        }
                        case "date" -> {
                            String manyKeyword = (String) row.get(3);
                            assertThat(manyKeyword, equalTo(manyDate));
                            long manyLong = (long) row.get(4);
                            assertThat(manyLong, equalTo(singleValue));
                        }
                        case "date_nanos" -> {
                            String manyKeyword = (String) row.get(3);
                            assertThat(manyKeyword, equalTo(manyDate));
                            long manyLong = (long) row.get(4);
                            assertThat(manyLong, equalTo(singleValue * 1000_000L));
                        }
                    }
                }
            }
        }
    }
}
