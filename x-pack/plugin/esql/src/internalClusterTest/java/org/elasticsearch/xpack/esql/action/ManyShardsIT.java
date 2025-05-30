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
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
            String index = "test-" + i;
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.shard.check_on_startup", "false")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("user", "type=keyword", "tags", "type=keyword")
                .get();
            BulkRequestBuilder bulk = client().prepareBulk(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int numDocs = between(10, 25); // every shard has at least 1 doc
            for (int d = 0; d < numDocs; d++) {
                String user = randomFrom("u1", "u2", "u3");
                String tag = randomFrom("java", "elasticsearch", "lucene");
                bulk.add(client().prepareIndex().setSource(Map.of("user", user, "tags", tag)));
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
                try (var response = run("from test-* | stats count(user) by tags", new QueryPragmas(pragmas.build()))) {
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

    public void testLimitCombineSmallerPages() {
        QueryPragmas queryPragmas = randomPragmas();
        if (canUseQueryPragmas()) {
            Settings.Builder settings = Settings.builder().put(queryPragmas.getSettings());
            settings.remove(QueryPragmas.NODE_LEVEL_REDUCTION.getKey());
            settings.remove(QueryPragmas.PAGE_SIZE.getKey());
            queryPragmas = new QueryPragmas(settings.build());
        }
        var request = new EsqlQueryRequest();
        request.query("FROM test-* | KEEP user | LIMIT 100");
        request.pragmas(queryPragmas);
        request.profile(true);
        try (EsqlQueryResponse resp = run(request)) {
            List<DriverProfile> nodeReduce = resp.profile().drivers().stream().filter(s -> s.description().equals("node_reduce")).toList();
            for (DriverProfile driverProfile : nodeReduce) {
                if (driverProfile.operators().size() == 2) {
                    continue; // when the target node is also the coordinator node
                }
                assertThat(driverProfile.operators(), hasSize(3));
                OperatorStatus exchangeSink = driverProfile.operators().get(2);
                assertThat(exchangeSink.status(), instanceOf(ExchangeSinkOperator.Status.class));
                ExchangeSinkOperator.Status exchangeStatus = (ExchangeSinkOperator.Status) exchangeSink.status();
                assertThat(exchangeStatus.pagesReceived(), lessThanOrEqualTo(1));
            }
            assertThat(resp.pages(), hasSize(1));
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
                QueryPragmas pragmas = randomPragmas();
                for (SearchService searchService : searchServices) {
                    SearchContextCounter counter = new SearchContextCounter(pragmas.maxConcurrentShardsPerNode());
                    var mockSearchService = (MockSearchService) searchService;
                    mockSearchService.setOnPutContext(r -> counter.onNewContext());
                    mockSearchService.setOnRemoveContext(r -> counter.onContextReleased());
                }
                run(q, pragmas).close();
            }
        } finally {
            for (SearchService searchService : searchServices) {
                var mockSearchService = (MockSearchService) searchService;
                mockSearchService.setOnPutContext(r -> {});
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

        var query = EsqlQueryRequest.syncEsqlQueryRequest();
        query.query("from test-* | LIMIT 1");
        query.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1).build()));

        try (var result = safeGet(client().execute(EsqlQueryAction.INSTANCE, query))) {
            assertThat(Iterables.size(result.rows()), equalTo(1L));
            assertThat(exchanges.get(), lessThanOrEqualTo(2));
        } finally {
            coordinatorNodeTransport.clearAllRules();
        }
    }
}
