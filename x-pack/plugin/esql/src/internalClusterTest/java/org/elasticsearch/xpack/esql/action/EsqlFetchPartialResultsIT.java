/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.esql.fetch.FetchOperator;
import org.elasticsearch.xpack.esql.fetch.FetchRequest;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Queries whose fetch fails for some shards. With {@code allow_partial_results} they return the rows of the other shards
 * and report the failed ones, like {@code _search}. Without it they fail.
 * <p>
 * Most tests query two indices whose rows alternate in the order of the sort, so both have winners, and the fetch of the
 * shard of one of them fails.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class EsqlFetchPartialResultsIT extends AbstractEsqlIntegTestCase {
    private static final String LOCAL = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    /** Documents with even {@code ts}, whose fetch works. */
    private static final String KEPT = "fetch_kept";
    /** Documents with odd {@code ts}, on one shard, whose fetch fails. */
    private static final String LOST = "fetch_lost";
    private static final int DOCS_PER_INDEX = 20;
    private static final int LIMIT = 10;
    private static final String QUERY = "FROM " + KEPT + "," + LOST + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";

    private final List<Runnable> removeMessageListeners = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(SimplePauseFieldPlugin.class);
        // the action that deletes a stored async result
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        return plugins;
    }

    @Before
    public void checkTheFetchPhaseIsAvailable() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        SimplePauseFieldPlugin.resetPlugin();
    }

    @After
    public void clearTransportRules() {
        SimplePauseFieldPlugin.release();
        removeMessageListeners.forEach(Runnable::run);
        removeMessageListeners.clear();
        for (String node : internalCluster().getNodeNames()) {
            mockTransport(node).clearAllRules();
        }
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {});
            mock.setOnRemoveContext(context -> {});
        }
    }

    /**
     * A reader context freed before the fetch drops the rows of its shard. The rows of the other shards come back in the
     * order of the sort, and the response is partial, with the shard listed as failed.
     */
    public void testAMissingContextDropsTheRowsOfItsShard() {
        createIndices();
        freeContextsBeforeTheirFetch(shardId -> shardId.getIndexName().equals(LOST));
        try (EsqlQueryResponse response = run(request(QUERY, true))) {
            assertThat(getValuesList(response), equalTo(rowsOfKept()));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertFailuresOf(local, LOST, SearchContextMissingException.class);
        }
    }

    /**
     * A data node that stops between the query phase and the fetch drops the rows of its shards, and the response is
     * partial.
     */
    public void testAStoppedNodeDropsTheRowsOfItsShards() throws Exception {
        String stopped = internalCluster().startDataOnlyNode();
        String coordinator = randomValueOtherThan(stopped, () -> randomFrom(internalCluster().getNodeNames()));
        createIndex(KEPT, indexSettings(between(1, 3), 0).put("index.routing.allocation.exclude._name", stopped), 0);
        createIndex(LOST, indexSettings(1, 0).put("index.routing.allocation.require._name", stopped), 1);
        ensureGreen(KEPT, LOST);

        CountDownLatch queryPhaseAnswered = queryPhaseAnswered(coordinator, stopped);
        CountDownLatch fetchSent = new CountDownLatch(1);
        CountDownLatch nodeStopped = new CountDownLatch(1);
        mockTransport(coordinator).addSendBehavior(
            internalCluster().getInstance(TransportService.class, stopped),
            (connection, requestId, action, request, options) -> {
                if (action.equals(FetchService.FETCH_ACTION_NAME)) {
                    fetchSent.countDown();
                    safeAwait(nodeStopped);
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        ActionFuture<EsqlQueryResponse> query = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(QUERY, true));
        try {
            safeAwait(queryPhaseAnswered);
            safeAwait(fetchSent);
            internalCluster().stopNode(stopped);
        } finally {
            nodeStopped.countDown();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), equalTo(rowsOfKept()));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertFailuresOf(local, LOST, ConnectTransportException.class);
        }
    }

    /**
     * Without {@code allow_partial_results}, a missing context fails the query with 404, like {@code _search}.
     */
    public void testWithoutPartialResultsAMissingContextFailsWithNotFound() {
        createIndices();
        freeContextsBeforeTheirFetch(shardId -> shardId.getIndexName().equals(LOST));
        Exception e = expectThrows(Exception.class, () -> run(request(QUERY, false)).close());
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause, instanceOf(SearchContextMissingException.class));
        assertThat(ExceptionsHelper.status(cause), equalTo(RestStatus.NOT_FOUND));
    }

    /**
     * An index deleted between the query phase and the fetch is a shard failure, and the rows of the other indices come
     * back.
     */
    public void testADeletedIndexIsAShardFailure() {
        createIndices();
        // the deletion is acknowledged before the shard closes and frees its contexts
        Set<ShardSearchContextId> lostContexts = ConcurrentHashMap.newKeySet();
        CountDownLatch lostContextFreed = new CountDownLatch(1);
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            MockSearchService mock = (MockSearchService) searchService;
            mock.setOnPutContext(context -> {
                if (context.indexShard().shardId().getIndexName().equals(LOST)) {
                    lostContexts.add(context.id());
                }
            });
            mock.setOnRemoveContext(context -> {
                if (lostContexts.contains(context.id())) {
                    lostContextFreed.countDown();
                }
            });
        }
        CountDownLatch fetchArrived = new CountDownLatch(1);
        CountDownLatch indexDeleted = new CountDownLatch(1);
        for (String node : internalCluster().getNodeNames()) {
            mockTransport(node).<FetchRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> {
                    fetchArrived.countDown();
                    safeAwait(indexDeleted);
                    handler.messageReceived(request, channel, task);
                }
            );
        }
        ActionFuture<EsqlQueryResponse> query = client().execute(EsqlQueryAction.INSTANCE, request(QUERY, true));
        try {
            safeAwait(fetchArrived);
            assertAcked(indicesAdmin().prepareDelete(LOST));
            safeAwait(lostContextFreed);
        } finally {
            indexDeleted.countDown();
        }
        try (EsqlQueryResponse response = query.actionGet(30, TimeUnit.SECONDS)) {
            assertThat(getValuesList(response), equalTo(rowsOfKept()));
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertFailuresOf(local, LOST, SearchContextMissingException.class);
        }
    }

    /**
     * When every fetch fails, the query returns no rows. A shard without winners still succeeded, so the response is
     * partial and lists the failures.
     */
    public void testWhenEveryFetchFailsTheResultIsEmptyAndPartial() {
        createIndices();
        String cold = "fetch_cold";
        // its rows sort after every other row, so its shard keeps no winner and gets no fetch
        createIndex(cold, indexSettings(1, 0), -10 * DOCS_PER_INDEX);
        ensureGreen(cold);
        freeContextsBeforeTheirFetch(shardId -> true);
        String query = "FROM " + KEPT + "," + LOST + "," + cold + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";
        try (EsqlQueryResponse response = run(request(query, true))) {
            assertThat(getValuesList(response), empty());
            assertTrue(response.isPartial());
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat("the shard without winners", local.getSuccessfulShards(), greaterThanOrEqualTo(1));
            assertThat(local.getFailures(), not(empty()));
            for (ShardSearchFailure failure : local.getFailures()) {
                assertThat(failure.getCause(), instanceOf(SearchContextMissingException.class));
            }
        }
    }

    /**
     * When the fetch of every shard fails, no shard of the query succeeded. The query fails like {@code _search} when all
     * its shards failed, even with partial results.
     */
    public void testWhenEveryShardFailsTheQueryFails() {
        // one shard per index, and their rows alternate, so every shard has winners
        createIndex(KEPT, indexSettings(1, 0), 0);
        createIndex(LOST, indexSettings(1, 0), 1);
        ensureGreen(KEPT, LOST);
        freeContextsBeforeTheirFetch(shardId -> true);
        Exception e = expectThrows(Exception.class, () -> run(request(QUERY, true)).close());
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(SearchContextMissingException.class));
    }

    /**
     * The cluster details count the shards whose fetch failed as failed, not as successful, and keep the total.
     */
    public void testTheClusterCountsTheShardsWhoseFetchFailed() {
        int keptShards = createIndices();
        freeContextsBeforeTheirFetch(shardId -> shardId.getIndexName().equals(LOST));
        try (EsqlQueryResponse response = run(request(QUERY, true))) {
            EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster(LOCAL);
            assertThat(local.getTotalShards(), equalTo(keptShards + 1));
            assertThat(local.getSuccessfulShards(), equalTo(keptShards));
            assertThat(local.getSkippedShards(), equalTo(0));
            assertThat(local.getFailedShards(), equalTo(1));
        }
    }

    /**
     * An async STOP during the cut fetches the rows selected so far, and the response is partial.
     */
    public void testStopFetchesTheRowsSelectedSoFar() throws Exception {
        List<String> dataNodes = new ArrayList<>();
        for (DiscoveryNode node : internalCluster().clusterService().state().nodes().getDataNodes().values()) {
            dataNodes.add(node.getName());
        }
        Collections.shuffle(dataNodes, random());
        String fastNode = dataNodes.get(0);
        String slowNode = dataNodes.get(1);
        String coordinator = dataNodes.get(2);
        String fast = "fetch_fast";
        String slow = "fetch_slow";
        assertAcked(
            indicesAdmin().prepareCreate(fast)
                .setSettings(indexSettings(between(1, 3), 0).put("index.routing.allocation.require._name", fastNode))
                .setMapping("ts", "type=long", "payload", "type=keyword", "pause", "type=long")
        );
        assertAcked(
            indicesAdmin().prepareCreate(slow)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", slowNode))
                .setMapping(pausingMapping())
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS_PER_INDEX; i++) {
            bulk.add(prepareIndex(fast).setSource("ts", i, "payload", "payload-" + i, "pause", 0));
            // without the stop these rows would win
            bulk.add(prepareIndex(slow).setSource("ts", 100 + i, "payload", "payload-" + (100 + i)));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(fast, slow);

        CountDownLatch fastAnswered = queryPhaseAnswered(coordinator, fastNode);
        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest(
            "FROM " + fast + "," + slow + " | WHERE pause >= 0 | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload"
        ).pragmas(fetchPhase()).allowPartialResults(true);
        request.profile(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.keepOnCompletion(true);
        String id;
        try (EsqlQueryResponse initial = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS)) {
            assertTrue(initial.isRunning());
            id = initial.asyncExecutionId().orElseThrow();
        }
        try {
            assertTrue("the slow index pauses", SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
            // the node of the fast index answers once the coordinator fetched all its pages
            safeAwait(fastAnswered);
            ActionFuture<EsqlQueryResponse> stop = client(coordinator).execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(id));
            assertBusy(() -> {
                try (EsqlQueryResponse running = EsqlAsyncTestUtils.getAsyncResponse(client(coordinator), id)) {
                    assertTrue(running.getExecutionInfo().isStopped());
                }
                // otherwise the paused driver could send rows once it resumes, before it sees the stop
                assertSinksFinished(slowNode);
            });
            SimplePauseFieldPlugin.release();
            try (EsqlQueryResponse stopped = stop.actionGet(30, TimeUnit.SECONDS)) {
                List<List<Object>> expected = new ArrayList<>();
                for (long ts = DOCS_PER_INDEX - 1; ts >= DOCS_PER_INDEX - LIMIT; ts--) {
                    expected.add(List.of(ts, "payload-" + ts));
                }
                assertThat(getValuesList(stopped), equalTo(expected));
                assertTrue(stopped.isPartial());
                assertThat("the payloads came from the fetch", fetchedDocuments(stopped), equalTo((long) LIMIT));
            }
        } finally {
            SimplePauseFieldPlugin.release();
            EsqlAsyncTestUtils.deleteAsyncId(client(coordinator), id);
        }
    }

    /**
     * Creates {@link #KEPT} on a few shards and {@link #LOST} on one.
     *
     * @return the shards of {@link #KEPT}
     */
    private int createIndices() {
        int keptShards = between(1, 3);
        createIndex(KEPT, indexSettings(keptShards, 0), 0);
        createIndex(LOST, indexSettings(1, 0), 1);
        ensureGreen(KEPT, LOST);
        return keptShards;
    }

    /**
     * Indexes {@link #DOCS_PER_INDEX} documents whose {@code ts} start at {@code firstTs} and grow by two, so the rows of an
     * index with even and one with odd {@code ts} alternate in the order of the sort.
     */
    private void createIndex(String index, Settings.Builder settings, long firstTs) {
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(settings).setMapping("ts", "type=long", "payload", "type=keyword"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS_PER_INDEX; i++) {
            long ts = firstTs + 2L * i;
            bulk.add(prepareIndex(index).setSource("ts", ts, "payload", "payload-" + ts));
        }
        assertFalse(bulk.get().hasFailures());
    }

    /**
     * The rows of {@link #QUERY} that come from {@link #KEPT}: the even {@code ts} among the winners.
     */
    private static List<List<Object>> rowsOfKept() {
        List<List<Object>> rows = new ArrayList<>();
        for (long ts = 2L * DOCS_PER_INDEX - 1; ts >= 2L * DOCS_PER_INDEX - LIMIT; ts--) {
            if (ts % 2 == 0) {
                rows.add(List.of(ts, "payload-" + ts));
            }
        }
        return rows;
    }

    private static EsqlQueryRequest request(String query, boolean allowPartialResults) {
        return syncEsqlQueryRequest(query).pragmas(fetchPhase()).allowPartialResults(allowPartialResults);
    }

    private static QueryPragmas fetchPhase() {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build());
    }

    /**
     * Frees the reader context of every shard that {@code shards} accepts just before the node that holds it serves a
     * fetch for it, as if its keep-alive passed between the query phase and the fetch.
     */
    private void freeContextsBeforeTheirFetch(Predicate<ShardId> shards) {
        for (String node : internalCluster().getNodeNames()) {
            SearchService searchService = internalCluster().getInstance(SearchService.class, node);
            mockTransport(node).<FetchRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> {
                    for (FetchRequest.ShardDocs shard : request.shards()) {
                        if (shards.test(shard.shardId())) {
                            searchService.freeReaderContext(shard.contextId());
                        }
                    }
                    handler.messageReceived(request, channel, task);
                }
            );
        }
    }

    /**
     * Counts down once {@code coordinator} receives the answer of {@code dataNode} to the query phase. A data node answers
     * once the coordinator fetched all its pages.
     */
    private CountDownLatch queryPhaseAnswered(String coordinator, String dataNode) {
        CountDownLatch answered = new CountDownLatch(1);
        TransportMessageListener listener = new TransportMessageListener() {
            @Override
            @SuppressWarnings("rawtypes")
            public void onResponseReceived(long requestId, Transport.ResponseContext context, int networkMessageSize) {
                if (context != null
                    && context.action().equals(ComputeService.DATA_ACTION_NAME)
                    && context.connection().getNode().getName().equals(dataNode)) {
                    answered.countDown();
                }
            }
        };
        MockTransportService transport = mockTransport(coordinator);
        transport.addMessageListener(listener);
        removeMessageListeners.add(() -> transport.removeMessageListener(listener));
        return answered;
    }

    private static void assertFailuresOf(EsqlExecutionInfo.Cluster cluster, String index, Class<? extends Throwable> cause) {
        assertThat(cluster.getFailures(), not(empty()));
        for (ShardSearchFailure failure : cluster.getFailures()) {
            assertThat(failure.index(), equalTo(index));
            assertThat(failure.getCause(), instanceOf(cause));
        }
    }

    private static void assertSinksFinished(String node) {
        ExchangeService exchangeService = internalCluster().getInstance(TransportEsqlQueryAction.class, node).exchangeService();
        for (String sinkId : exchangeService.sinkKeys()) {
            try {
                assertTrue("sink " + sinkId + " on " + node + " not finished", exchangeService.getSinkHandler(sinkId).isFinished());
            } catch (ResourceNotFoundException e) {
                // the sink completed and went away
            }
        }
    }

    /**
     * The documents the fetch loaded on the coordinator, from the profile.
     */
    private static long fetchedDocuments(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .flatMap(driver -> driver.operators().stream())
            .map(OperatorStatus::status)
            .filter(FetchOperator.Status.class::isInstance)
            .mapToLong(status -> ((FetchOperator.Status) status).documents())
            .sum();
    }

    /**
     * {@code ts} and {@code payload}, and a {@code pause} field whose script waits until {@link SimplePauseFieldPlugin}
     * lets it go on.
     */
    private static XContentBuilder pausingMapping() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("pause");
            mapping.field("type", "long");
            mapping.startObject("script").field("source", "").field("lang", "pause").endObject();
            mapping.endObject();
        }
        mapping.endObject();
        mapping.startObject("properties");
        {
            mapping.startObject("ts").field("type", "long").endObject();
            mapping.startObject("payload").field("type", "keyword").endObject();
        }
        mapping.endObject();
        return mapping.endObject();
    }

    private static MockTransportService mockTransport(String node) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, node);
    }
}
