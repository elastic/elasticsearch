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
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.STATE_MACHINE_RETRY_DELAY;
import static org.hamcrest.Matchers.instanceOf;

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

    @Override
    protected boolean addMockFsRepository() {
        // This test is pretty involved, creating up to 10 index nodes, and can hit the 10-second timeout in
        // ConcurrentMultiPartUploadsMockFsRepository when using a 5-thread thread pool to write chunks
        return false;
    }

    private static class IndexedDocument {
        String id;
        // We need to keep a ledger of all field value updates since untracked refreshes can occur, such as during handoff
        // The last entry is the most recent value
        List<String> pendingFieldValues;
        // The field value on the segment after successfully refreshing
        @Nullable
        String lastRefreshedValue;
        @Nullable
        String routing;
        // Don't clear the pending field values ledger upon a refresh if this flag is set
        // During disruptions we record every update even if they fail, so it's possible to set a false segment field value
        boolean keepFullHistory;

        IndexedDocument(String id, String fieldValue, @Nullable String routing, boolean keepFullHistory) {
            this.id = id;
            this.pendingFieldValues = new ArrayList<>(List.of(fieldValue));
            this.lastRefreshedValue = null;
            this.routing = routing;
            this.keepFullHistory = keepFullHistory;
        }

        boolean wasRefreshed() {
            return lastRefreshedValue != null;
        }

        // Assert the fieldValue was a certain value
        // Used in search and non-real-time gets when an untracked refresh could have occurred
        void assertHadFieldValue(String fieldValue) {
            assertTrue(
                "Failed to find field value ["
                    + fieldValue
                    + "] for ["
                    + id
                    + "] with pendingFieldValues="
                    + pendingFieldValues
                    + " segmentFieldValue=["
                    + lastRefreshedValue
                    + "]",
                fieldValue.equals(lastRefreshedValue) || pendingFieldValues.contains(fieldValue)
            );
        }

        String latestFieldValue() {
            return pendingFieldValues.isEmpty() ? lastRefreshedValue : pendingFieldValues.getLast();
        }

        void updateFieldValue(String fieldValue) {
            pendingFieldValues.add(fieldValue);
        }

        void refresh() {
            if (pendingFieldValues.isEmpty() == false) {
                lastRefreshedValue = pendingFieldValues.getLast();
                if (keepFullHistory == false) {
                    pendingFieldValues.clear();
                }
            }
        }
    }

    private class NoDisruptionExecutor implements PerThreadOperationExecutor {
        private final HashMap<String, IndexedDocument> indexed = new HashMap<>();

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
                    indexed.values().forEach(IndexedDocument::refresh);
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
                        indexed.forEach((documentId, document) -> {
                            // Everything we've indexed and refreshed should be present.
                            // Other threads are working too so there will be more but shouldn't be less.
                            if (document.wasRefreshed()) {
                                var fieldValue = fieldValueInHits.get(documentId);
                                assertNotNull(fieldValue);
                                document.assertHadFieldValue(fieldValue);
                            }
                        });
                    });
                }
                case INDEX -> {
                    // we assume these are less used than bulks
                    boolean useIndexApi = randomDouble() < 0.1;
                    if (useIndexApi) {
                        String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);

                        String documentId = "document" + id;
                        String routing = randomBoolean() ? null : randomAlphaOfLength(5);
                        indexed.put(documentId, new IndexedDocument(documentId, fieldValue, routing, false));
                        id += 1;
                        var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue, routing);
                        var indexResponse = indexRequest.get();
                        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                    } else {
                        int bulkSize = randomIntBetween(1, 20);
                        final var client = client(coordinatorNode);
                        var bulkRequest = client.prepareBulk();
                        for (int j = 0; j < bulkSize; j++) {
                            String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);

                            String documentId = "document" + id;
                            String routing = randomBoolean() ? null : randomAlphaOfLength(5);
                            indexed.put(documentId, new IndexedDocument(documentId, fieldValue, routing, false));
                            id += 1;

                            var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue, routing);
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
                case GET -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    IndexedDocument document = randomFrom(indexed.values());
                    boolean realTime = randomBoolean();
                    var response = client(coordinatorNode).prepareGet(indexName, document.id)
                        .setRealtime(realTime)
                        .setRouting(document.routing)
                        .execute()
                        .actionGet();
                    if (realTime || document.wasRefreshed()) {
                        assertTrue(response.isExists());
                    }
                    if (response.isExists()) {
                        String fieldValue = (String) response.getSourceAsMap().get("field");
                        if (realTime) {
                            assertEquals(document.latestFieldValue(), fieldValue);
                        } else {
                            document.assertHadFieldValue(fieldValue);
                        }
                    }
                }
                case MULTIGET -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    boolean realTime = randomBoolean();
                    var multiget = client(coordinatorNode).prepareMultiGet().setRealtime(realTime);
                    int numDocs = randomIntBetween(1, 5);
                    List<IndexedDocument> documents = new ArrayList<>(numDocs);
                    for (int i = 0; i < numDocs; i++) {
                        IndexedDocument document = randomFrom(indexed.values());
                        var get = new MultiGetRequest.Item(indexName, document.id).routing(document.routing);
                        multiget.add(get);
                        documents.add(document);
                    }
                    var responses = multiget.execute().actionGet().getResponses();
                    for (int i = 0; i < numDocs; i++) {
                        var response = responses[i];
                        if (response.isFailed()) {
                            var failure = response.getFailure().getFailure();
                            assertThat(failure, instanceOf(StaleRequestException.class));
                            // Rethrow to retry in executeOperations
                            throw (StaleRequestException) failure;
                        } else {
                            IndexedDocument document = documents.get(i);
                            var getResponse = response.getResponse();
                            if (realTime || document.wasRefreshed()) {
                                assertTrue(getResponse.isExists());
                            }
                            if (getResponse.isExists()) {
                                String fieldValue = (String) getResponse.getSourceAsMap().get("field");
                                if (realTime) {
                                    assertEquals(document.latestFieldValue(), fieldValue);
                                } else {
                                    document.assertHadFieldValue(fieldValue);
                                }
                            }
                        }
                    }
                }
                case TERM_VECTOR -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    IndexedDocument document = randomFrom(indexed.values());
                    boolean realTime = randomBoolean();
                    var response = client(coordinatorNode).prepareTermVectors(indexName, document.id)
                        .setRealtime(realTime)
                        .setRouting(document.routing)
                        .execute()
                        .actionGet();
                    if (realTime || document.wasRefreshed()) {
                        assertTrue(response.isExists());
                    }
                    if (response.isExists()) {
                        try {
                            assertEquals(1, response.getFields().size());
                            assertEquals("field", response.getFields().iterator().next());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                case UPDATE -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    IndexedDocument document = randomFrom(indexed.values());
                    String newFieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);
                    var response = client(coordinatorNode).prepareUpdate(indexName, document.id)
                        .setDoc("field", newFieldValue)
                        .setRouting(document.routing)
                        .setDetectNoop(false)
                        .execute()
                        .actionGet();
                    assertEquals(DocWriteResponse.Result.UPDATED, response.getResult());
                    document.updateFieldValue(newFieldValue);
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
            }
        }
    }

    private class UnderDisruptionExecutor implements PerThreadOperationExecutor {
        private final HashMap<String, IndexedDocument> indexed = new HashMap<>();

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
                        indexed.values().forEach(IndexedDocument::refresh);
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
                            fieldValueInHits.forEach((documentId, fieldValue) -> {
                                var document = indexed.get(documentId);
                                assertNotNull(document);
                                document.assertHadFieldValue(fieldValue);
                            });
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
                        String routing = randomBoolean() ? null : randomAlphaOfLength(5);
                        var document = new IndexedDocument(documentId, fieldValue, routing, true);
                        currentId += 1;
                        indexed.put(documentId, document);

                        var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue, routing);
                        try {
                            DocWriteResponse response = indexRequest.execute().actionGet();
                            assertTrue(
                                response.getResult().name(),
                                response.getResult() == DocWriteResponse.Result.CREATED
                                    || response.getResult() == DocWriteResponse.Result.UPDATED
                            );
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
                            String routing = randomBoolean() ? null : randomAlphaOfLength(5);
                            var document = new IndexedDocument(documentId, fieldValue, routing, true);
                            currentId += 1;
                            // Bulk requests can partially fail due to a node restart or something else after the data is already
                            // in the translog.
                            // Such writes will be successful and so have to assume all writes can succeed.
                            indexed.put(documentId, document);

                            var indexRequest = createIndexRequest(coordinatorNode, indexName, documentId, fieldValue, routing);
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
                case GET -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    IndexedDocument document = randomFrom(indexed.values());
                    boolean realTime = randomBoolean();
                    try {
                        var response = client(coordinatorNode).prepareGet(indexName, document.id)
                            .setRealtime(realTime)
                            .setRouting(document.routing)
                            .execute()
                            .actionGet();
                        // It is possible that the corresponding write operation actually failed so this is expected
                        if (response.isExists()) {
                            String fieldValue = (String) response.getSourceAsMap().get("field");
                            // Check full history of updates since we record all updates even if they fail
                            document.assertHadFieldValue(fieldValue);
                        }
                    } catch (ElasticsearchException e) {
                        // Shard unavailable during disruption
                    }
                }
                case MULTIGET -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    boolean realTime = randomBoolean();
                    var multiget = client(coordinatorNode).prepareMultiGet().setRealtime(realTime);
                    int numDocs = randomIntBetween(1, 5);
                    List<IndexedDocument> documents = new ArrayList<>(numDocs);
                    for (int i = 0; i < numDocs; i++) {
                        IndexedDocument document = randomFrom(indexed.values());
                        var get = new MultiGetRequest.Item(indexName, document.id).routing(document.routing);
                        multiget.add(get);
                        documents.add(document);
                    }
                    var responses = multiget.execute().actionGet().getResponses();
                    for (int i = 0; i < numDocs; i++) {
                        var response = responses[i];
                        if (response.isFailed() && response.getFailure().getFailure() instanceof StaleRequestException sre) {
                            // Rethrow to retry in executeOperations
                            throw sre;
                        }
                        var getResponse = response.getResponse();
                        // Shard may be unavailable during disruption
                        if (response.isFailed() == false && getResponse.isExists()) {
                            IndexedDocument document = documents.get(i);
                            String fieldValue = (String) getResponse.getSourceAsMap().get("field");
                            // Check full history of updates since we record all updates even if they fail
                            document.assertHadFieldValue(fieldValue);
                        }
                    }
                }
                case TERM_VECTOR -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    IndexedDocument document = randomFrom(indexed.values());
                    boolean realTime = randomBoolean();
                    try {
                        var response = client(coordinatorNode).prepareTermVectors(indexName, document.id)
                            .setRealtime(realTime)
                            .setRouting(document.routing)
                            .execute()
                            .actionGet();
                        if (response.isExists()) {
                            try {
                                assertEquals(1, response.getFields().size());
                                assertEquals("field", response.getFields().iterator().next());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } catch (ElasticsearchException e) {
                        // Shard may be unavailable during disruption
                    }
                }
                case UPDATE -> {
                    if (indexed.isEmpty()) {
                        return;
                    }
                    IndexedDocument document = randomFrom(indexed.values());
                    String newFieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);
                    // Update requests can fail to return a response after the data is already in the translog, so we have to assume all
                    // updates can succeed and record the value before sending
                    document.updateFieldValue(newFieldValue);
                    try {
                        var response = client(coordinatorNode).prepareUpdate(indexName, document.id)
                            .setDoc("field", newFieldValue)
                            .setRouting(document.routing)
                            .setDetectNoop(false)
                            .execute()
                            .actionGet();
                        assertEquals(DocWriteResponse.Result.UPDATED, response.getResult());
                    } catch (ElasticsearchException e) {
                        // Shard may be unavailable during disruption
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
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
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(shards, 1)
                    // Due to all the disruption we can hit the default maximum of 5.
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 100)
                    .build()
            ).setMapping("""
                {"properties":{"field":{"type": "text","term_vector":"yes","analyzer":"keyword"}}}
                """)
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

    private IndexRequestBuilder createIndexRequest(
        String coordinatorNode,
        String indexName,
        String documentId,
        String fieldValue,
        @Nullable String routing
    ) {
        var indexRequest = client(coordinatorNode).prepareIndex(indexName);
        indexRequest.setId(documentId);
        indexRequest.setSource(Map.of("field", fieldValue));
        indexRequest.setRouting(routing);
        return indexRequest;
    }

    private record WeightedOperation(Operation operation, int weight) {}

    private static final List<WeightedOperation> OPERATIONS = List.of(
        new WeightedOperation(Operation.INDEX, 50),
        new WeightedOperation(Operation.REFRESH, 10),
        new WeightedOperation(Operation.SEARCH, 20),
        new WeightedOperation(Operation.GET, 5),
        new WeightedOperation(Operation.MULTIGET, 5),
        new WeightedOperation(Operation.TERM_VECTOR, 5),
        new WeightedOperation(Operation.UPDATE, 5)
    );
    private static final int ROLL_SIZE = OPERATIONS.stream().mapToInt(WeightedOperation::weight).sum();
    static {
        assertEquals(100, ROLL_SIZE);
    }

    private List<Operation> randomOperations(int size) {
        var result = new ArrayList<Operation>(size);

        for (int i = 0; i < size; i++) {
            int roll = randomIntBetween(0, ROLL_SIZE - 1);
            for (WeightedOperation operation : OPERATIONS) {
                if (roll < operation.weight) {
                    result.add(operation.operation);
                    break;
                }
                roll -= operation.weight;
            }
        }

        assertEquals(size, result.size());
        return result;
    }

    enum Operation {
        INDEX,
        REFRESH,
        SEARCH,
        GET,
        MULTIGET,
        TERM_VECTOR,
        UPDATE
    }
}
