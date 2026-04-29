/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.STATE_MACHINE_RETRY_DELAY;

public class StatelessReshardMixedOperationsIT extends StatelessReshardDisruptionBaseIT {
    public void testMixedOperationsDuringSplit() throws Exception {
        runTest(NoDisruptionExecutor::new, Disruptor.NOOP);
    }

    public void testMixedOperationsDuringSplitWithDisruption() throws Exception {
        var disruptor = new Disruptor() {
            private final AtomicBoolean stop = new AtomicBoolean(false);
            private Thread thread;

            @Override
            public void start(Index index, int clusterSize, int shardCount, String coordinator) {
                var thread = new Thread(() -> {
                    do {
                        Failure randomFailure = randomFrom(Failure.values());
                        try {
                            induceFailure(randomFailure, index, coordinator);
                        } catch (Exception e) {
                            logger.error("Error in disruption thread", e);
                            throw new RuntimeException(e);
                        }
                    } while (stop.get() == false);
                });
                thread.start();

                this.thread = thread;
            }

            @Override
            public void stop() throws Exception {
                stop.set(true);
                this.thread.join();
            }
        };

        runTest(UnderDisruptionExecutor::new, disruptor);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // Test framework randomly sets this to 0, but we rely on retries to handle target shards still being in recovery
        // when we start re-splitting bulk requests.
        return super.nodeSettings().put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s")
            // Reduce the grace period to speed up the test.
            // We should not see requests that were queued for a long time in a local cluster setup anyway.
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.timeValueMillis(100))
            // Reduce the delay between retries to speed up the test.
            .put(STATE_MACHINE_RETRY_DELAY.getKey(), TimeValue.timeValueMillis(10));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), AddSettingPlugin.class);
    }

    public static class AddSettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(STATE_MACHINE_RETRY_DELAY);
        }
    }

    private class NoDisruptionExecutor implements PerThreadOperationExecutor {
        private final HashMap<String, String> indexed = new HashMap<>();
        private final HashMap<String, String> indexedAndRefreshed = new HashMap<>();

        private String indexName;
        private String coordinatorNode;
        private int id;

        @Override
        public void initialize(String indexName, int threadIndex, int threadCount, String coordinatorNode) {
            this.indexName = indexName;
            this.coordinatorNode = coordinatorNode;
            // Prevent other threads from updating our documents since then we wouldn't be able to do asserts.
            this.id = Integer.MAX_VALUE / threadCount * threadIndex;
        }

        @Override
        public void execute(Operation operation) {
            switch (operation) {
                case REFRESH -> {
                    var refreshResult = client(coordinatorNode).admin().indices().prepareRefresh(indexName).get();
                    assertEquals(Arrays.toString(refreshResult.getShardFailures()), 0, refreshResult.getFailedShards());

                    indexedAndRefreshed.putAll(indexed);
                    indexed.clear();
                }
                case SEARCH -> {
                    var search = client(coordinatorNode).prepareSearch(indexName)
                        // We expect resharding to be seamless.
                        .setAllowPartialSearchResults(false)
                        .setQuery(QueryBuilders.matchAllQuery())
                        .setSize(10000);
                    assertResponse(search, r -> {
                        Map<String, String> fieldValueInHits = Arrays.stream(r.getHits().getHits())
                            .collect(Collectors.toMap(SearchHit::getId, h -> (String) h.getSourceAsMap().get("field")));
                        for (var entry : indexedAndRefreshed.entrySet()) {
                            // Everything we've indexed and refreshed should be present.
                            // Other threads are working too so there will be more but shouldn't be less.
                            var fieldValue = fieldValueInHits.get(entry.getKey());
                            assertEquals(entry.getValue(), fieldValue);
                        }
                    });
                }
                case INDEX -> {
                    // we assume these are less used than bulks
                    boolean useIndexApi = randomDouble() < 0.1;
                    if (useIndexApi) {
                        String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);

                        String documentId = "document" + id;
                        indexed.put(documentId, fieldValue);
                        id += 1;

                        var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue);
                        var indexResponse = indexRequest.get();
                        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                    } else {
                        int bulkSize = randomIntBetween(1, 20);
                        final var client = client(coordinatorNode);
                        var bulkRequest = client.prepareBulk();
                        for (int j = 0; j < bulkSize; j++) {
                            String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);

                            String documentId = "document" + id;
                            indexed.put(documentId, fieldValue);
                            id += 1;

                            var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue);
                            bulkRequest.add(indexRequest);
                        }
                        var bulkResponse = bulkRequest.get();
                        if (bulkResponse.hasFailures()) {
                            var message = new StringBuilder("Bulk request failed. Failures:\n");
                            for (var response : bulkResponse) {
                                if (response.isFailed()) {
                                    message.append(ExceptionsHelper.unwrapCause(response.getFailure().getCause()));
                                    message.append("\n-----\n");
                                }
                            }
                            throw new AssertionError(message);
                        }
                    }
                }
            }
        }
    }

    private class UnderDisruptionExecutor implements PerThreadOperationExecutor {
        private final HashMap<String, String> allIndexedDocuments = new HashMap<>();
        private final HashMap<String, String> indexedSinceLastRefresh = new HashMap<>();
        private final HashMap<String, String> indexedAndRefreshed = new HashMap<>();

        private String indexName;
        private String coordinatorNode;
        private Tuple<Integer, Integer> idRange;
        private int currentId;

        @Override
        public void initialize(String indexName, int threadIndex, int threadCount, String coordinatorNode) {
            this.indexName = indexName;
            this.coordinatorNode = coordinatorNode;
            // Prevent other threads from updating our documents since then we wouldn't be able to do asserts.
            this.idRange = new Tuple<>(Integer.MAX_VALUE / threadCount * threadIndex, Integer.MAX_VALUE / threadCount * (threadIndex + 1));
            this.currentId = idRange.v1();
        }

        @Override
        public void execute(Operation operation) {
            switch (operation) {
                case REFRESH -> {
                    BroadcastResponse refreshResult = client(coordinatorNode).admin().indices().prepareRefresh(indexName).get();
                    // Refresh can fail on some shards due to disruption.
                    // We'll assume nothing was refreshed in that case because otherwise it is not obvious
                    // how to map what documents are refreshed (since we need to know the state of split to reason about that).
                    if (refreshResult.getFailedShards() == 0) {
                        indexedAndRefreshed.putAll(indexedSinceLastRefresh);
                        indexedSinceLastRefresh.clear();
                    }
                }
                case SEARCH -> {
                    var search = client(coordinatorNode).prepareSearch(indexName)
                        // Shards can fail due to disruption.
                        .setAllowPartialSearchResults(true)
                        .setQuery(QueryBuilders.matchAllQuery())
                        .setSize(10000);

                    try {
                        var searchResponse = search.get();

                        try {
                            // Filter only documents that are in the id range of this thread.
                            // By doing this transformation we also assert that there are no duplicates in hits.
                            Map<String, String> fieldValueInHits = Arrays.stream(searchResponse.getHits().getHits()).filter(h -> {
                                int id = Integer.parseInt(h.getId().substring("document".length()));
                                return id >= idRange.v1() && id < idRange.v2();
                            }).collect(Collectors.toMap(SearchHit::getId, h -> (String) h.getSourceAsMap().get("field")));

                            // Partial refreshes are possible, but we only track fully succeeded ones.
                            // `BroadcastResponse` doesn't give you details on what shards succeeded, only the number of successful shards.
                            // So with such limited information this is the best we can do realistically.
                            // That being said it is possible we may see documents here that were indexed but not
                            // refreshed in the strict definition (there were no fully successful refreshes but may have been partial ones).
                            // So we check both refreshed and indexed documents.
                            for (var entry : fieldValueInHits.entrySet()) {
                                var fieldValue = Optional.ofNullable(indexedAndRefreshed.get(entry.getKey()))
                                    .or(() -> Optional.ofNullable(allIndexedDocuments.get(entry.getKey())));
                                String message = String.format(
                                    Locale.ROOT,
                                    "Expected to see document with field value %s but got %s",
                                    fieldValue,
                                    entry.getKey()
                                );
                                assertTrue(message, fieldValue.isPresent());
                                assertEquals(message, entry.getValue(), fieldValue.get());
                            }
                        } finally {
                            searchResponse.decRef();
                        }
                    } catch (ElasticsearchException e) {
                        // We can get "all shards failed" if all search shards are allocated on the same node
                        // or if there is one search node in total and it is down.
                        assertTrue(e.getMessage(), e.getMessage().contains("all shards failed"));
                    }
                }
                case INDEX -> {
                    // we assume these are less used than bulks
                    boolean useIndexApi = randomDouble() < 0.1;
                    if (useIndexApi) {
                        String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);

                        String documentId = "document" + currentId;
                        currentId += 1;
                        allIndexedDocuments.put(documentId, fieldValue);
                        indexedSinceLastRefresh.put(documentId, fieldValue);

                        var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue);
                        try {
                            DocWriteResponse response = indexRequest.execute().actionGet();
                            assertTrue(
                                response.getResult().name(),
                                response.getResult() == DocWriteResponse.Result.CREATED
                                    || response.getResult() == DocWriteResponse.Result.UPDATED
                            );
                            // We can see UPDATED if we retry an operation that failed but was already written to the translog.
                            if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                                // Since it's a retry we should never see versions higher than 2.
                                assertEquals(2, response.getVersion());
                            }
                        } catch (StaleRequestException e) {
                            // TODO
                            // We currently don't have grace period to drain queued requests and so can see this pretty often.
                        }
                    } else {
                        int bulkSize = randomIntBetween(1, 20);

                        final var client = client(coordinatorNode);
                        var bulkRequest = client.prepareBulk();
                        for (int j = 0; j < bulkSize; j++) {
                            String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);

                            String documentId = "document" + currentId;
                            currentId += 1;
                            // Bulk requests can partially fail due to a node restart or something else after the data is already
                            // in the translog.
                            // Such writes will be successful and so have to assume all writes can succeed.
                            allIndexedDocuments.put(documentId, fieldValue);
                            indexedSinceLastRefresh.put(documentId, fieldValue);

                            var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue);
                            bulkRequest.add(indexRequest);
                        }

                        try {
                            bulkRequest.get();
                        } catch (StaleRequestException e) {
                            // TODO
                            // We currently don't have grace period to drain queued requests and so can see this pretty often.
                        }
                    }
                }
            }
        }
    }

    private void runTest(Supplier<PerThreadOperationExecutor> executorSupplier, Disruptor disruptor) throws Exception {
        String masterNode = startMasterOnlyNode();

        // Dedicated coordinator node so that we don't get hard failures for example when coordinator is restarted.
        String dedicatedCoordinatorNode = startSearchNode();
        // Exclude coordinator from allocation.
        client().admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", dedicatedCoordinatorNode))
            .get();

        int shards = randomIntBetween(2, 5);

        int indexNodes = randomIntBetween(1, shards * 2);
        startIndexNodes(indexNodes);
        int searchNodes = randomIntBetween(1, shards * 2);
        startSearchNodes(searchNodes);

        int clusterSize = 1 + 1 + indexNodes + searchNodes;
        ensureStableCluster(clusterSize, masterNode);

        String indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(shards, 1)
                // Due to all the disruption we can hit the default maximum of 5.
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 100)
                .build()
        );
        Index index = resolveIndex(indexName);
        ensureGreen(indexName);

        int threadsCount = randomIntBetween(1, 10);
        var threads = new ArrayList<Thread>();

        // Let threads run for a bit so that we have some data to move around during split.
        var readyForSplit = new CountDownLatch(threadsCount);
        for (int i = 0; i < threadsCount; i++) {
            var executor = executorSupplier.get();
            executor.initialize(indexName, i, threadsCount, dedicatedCoordinatorNode);

            // We don't need a lot of operations since we'll block both indexing and refresh at some point during split.
            // And as a result most of them will be executed in the later stages of the split which is not that useful here.
            var threadOperations = randomOperations(randomIntBetween(10, 50));

            var thread = new Thread(() -> executeOperations(executor, threadOperations, readyForSplit));
            thread.start();
            threads.add(thread);
        }

        readyForSplit.await();

        logger.info("--> Starting disruption");
        disruptor.start(index, clusterSize, shards * 2, dedicatedCoordinatorNode);
        try {
            // TODO execute multiple rounds
            int splitRounds = 1;
            for (int i = 0; i < splitRounds; i++) {
                logger.info("--> Executing a split round");
                client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
                awaitClusterState(
                    masterNode,
                    (state) -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() != null
                );
                awaitClusterState(
                    masterNode,
                    (state) -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() == null
                );
                logger.info("--> Split round complete");
            }

            for (int i = 0; i < threadsCount; i++) {
                threads.get(i).join(SAFE_AWAIT_TIMEOUT.millis());
            }
        } finally {
            logger.info("--> Stopping disruption");
            disruptor.stop();
            ensureStableCluster(clusterSize, masterNode);
            logger.info("--> Disruptions stopped");
        }
    }

    private interface PerThreadOperationExecutor {
        void initialize(String indexName, int threadIndex, int threadCount, String coordinatorNode);

        void execute(Operation operation);
    }

    private interface Disruptor {
        void start(Index index, int clusterSize, int shardCount, String coordinator);

        void stop() throws Exception;

        Disruptor NOOP = new Disruptor() {
            @Override
            public void start(Index index, int clusterSize, int shardCount, String coordinator) {}

            @Override
            public void stop() {}
        };
    }

    private void executeOperations(PerThreadOperationExecutor executor, List<Operation> operations, CountDownLatch halfwayDone) {
        for (int i = 0; i < operations.size(); i++) {
            if (i == operations.size() / 2) {
                halfwayDone.countDown();
            }

            // We lower RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD to speed up these tests
            // but that also means that resharding completes quickly.
            // If some thread is slow to execute an operation with old shard count summary (maybe due to a big GC or something)
            // it can encouter `StaleRequestException` and that is expected.
            // This should be resolved with a single retry since if resharding is complete,
            // the coordinator must have the updated routing.
            for (int tries = 0; tries < 2; tries++) {
                try {
                    executor.execute(operations.get(i));
                    break;
                } catch (StaleRequestException ignored) {}
            }
        }
    }

    private IndexRequestBuilder createIndexRequest(String coordinatorNode, String indexName, String documentId, String fieldValue) {
        var indexRequest = client(coordinatorNode).prepareIndex(indexName);
        indexRequest.setId(documentId);
        indexRequest.setSource(Map.of("field", fieldValue));

        if (randomBoolean()) {
            indexRequest.setRouting(randomAlphaOfLength(5));
        }

        return indexRequest;
    }

    // Generates a list of random operations with some basic logic to make it somewhat realistic.
    private List<Operation> randomOperations(int size) {
        var result = new ArrayList<Operation>(size);

        for (int i = 0; i < size; i++) {
            int roll = randomIntBetween(0, 100);
            // Steady state of indexing.
            if (roll < 60) {
                result.add(Operation.INDEX);
            } else if (roll < 70) {
                // 10% percent refresh
                result.add(Operation.REFRESH);
            } else {
                // 30% search
                result.add(Operation.SEARCH);
            }
        }

        return result;
    }

    enum Operation {
        INDEX,
        REFRESH,
        SEARCH
    }
}
