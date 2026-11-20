/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * A get async search request that reaches a node other than the one running the search is relayed to that node. The relaying node
 * charges the response to its request breaker while it arrives, and then keeps it charged until it is released, so a response it has
 * no room for fails the request with a 429. The node running the search charges the reply it serializes for a relayed request until it
 * is written, so it fails the request with a 429 too when it has no room for the reply.
 * <p>
 * The search runs on a data node. The get requests go through a coordinating only node with a {@value #RELAY_REQUEST_LIMIT} request
 * breaker, which a response with {@value #DOCS} hits doesn't fit in, and one with {@value #HELD_DOCS} hits fits in only once.
 * {@link #testOwnerChargesTheSerializedReply} replaces that limit with a cluster setting.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AsyncSearchRelayCircuitBreakerIT extends AsyncSearchIntegTestCase {

    private static final String INDEX = "test";
    private static final int DOCS = 200;
    private static final int HELD_DOCS = 8;
    private static final int PAYLOAD_CHARS = 4000;
    private static final String RELAY_REQUEST_LIMIT = "50kb";

    private String owner;
    private String relay;

    private void startCluster() {
        // The test cluster can make a node's request breaker a noop one, which charges nothing.
        owner = internalCluster().startNode(Settings.builder().put("indices.breaker.request.type", "memory"));
        relay = internalCluster().startCoordinatingOnlyNode(
            Settings.builder()
                .put("indices.breaker.request.type", "memory")
                .put("indices.breaker.request.limit", RELAY_REQUEST_LIMIT)
                .build()
        );
        createIndex(INDEX, indexSettings(1, 0).build());
        BulkRequestBuilder bulk = client(owner).prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            bulk.add(prepareIndex(INDEX).setSource("payload", randomAlphaOfLength(PAYLOAD_CHARS)));
        }
        assertNoFailures(bulk.get());
        refresh(INDEX);
    }

    /**
     * A response that doesn't fit is rejected by the relaying node with a 429 while it arrives, and the search still completes. The
     * relaying node waits for the search to complete, so the response it gets is the one the task holds in memory.
     */
    public void testRelayedResponseTripsWhileArriving() throws Exception {
        startCluster();
        CircuitBreaker relayBreaker = internalCluster().getInstance(CircuitBreakerService.class, relay).getBreaker(CircuitBreaker.REQUEST);
        long tripsBefore = relayBreaker.getTrippedCount();

        final String id;
        try (BlockingQueryBuilder.QueryLatch latch = BlockingQueryBuilder.acquireQueryLatch(0)) {
            id = submitBlockingSearch(DOCS);
            ActionFuture<AsyncSearchResponse> relayed = client(relay).execute(
                GetAsyncSearchAction.INSTANCE,
                new GetAsyncResultRequest(id).setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30))
            );
            TaskManager ownerTasks = internalCluster().getInstance(TransportService.class, owner).getTaskManager();
            assertBusy(
                () -> assertTrue(
                    "the relayed get request did not reach the owning node",
                    ownerTasks.getTasks().values().stream().anyMatch(task -> task.getAction().equals(GetAsyncSearchAction.NAME))
                )
            );
            latch.close();

            // The trip itself, not wrapped in a remote transport exception, since it is the relaying node that has no room.
            Exception e = expectThrows(Exception.class, () -> relayed.actionGet(SAFE_AWAIT_TIMEOUT).decRef());
            assertThat(e, instanceOf(CircuitBreakingException.class));
            assertThat(((CircuitBreakingException) e).status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(e.getMessage(), containsString("[" + TransportGetAsyncSearchAction.RELAYED_RESPONSE_BREAKER_LABEL + "]"));
            // A piece trips on arrival, which wants far less than the whole response a trip once it is read would want.
            assertThat(((CircuitBreakingException) e).getBytesWanted(), lessThan((long) DOCS * PAYLOAD_CHARS / 2));
        }
        assertThat(relayBreaker.getTrippedCount(), greaterThan(tripsBefore));
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));

        // The search did not fail: the node that runs it still returns all hits.
        AsyncSearchResponse direct = client(owner).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
        try {
            assertFalse(direct.isRunning());
            assertThat(direct.getSearchResponse().getHits().getHits().length, equalTo(DOCS));
        } finally {
            direct.decRef();
        }
        deleteAsyncSearch(id);
    }

    /**
     * A relayed response stays charged until it is released, so one that fits leaves no room for a second one.
     */
    public void testRelayedResponseIsChargedUntilReleased() throws Exception {
        startCluster();
        CircuitBreaker relayBreaker = internalCluster().getInstance(CircuitBreakerService.class, relay).getBreaker(CircuitBreaker.REQUEST);

        final String id;
        try (BlockingQueryBuilder.QueryLatch latch = BlockingQueryBuilder.acquireQueryLatch(0)) {
            id = submitBlockingSearch(HELD_DOCS);
            latch.close();
            ensureTaskCompletion(id);
        }
        AsyncSearchResponse held = getThroughRelay(id);
        try {
            assertFalse(held.isRunning());
            assertThat(held.getSearchResponse().getHits().getHits().length, equalTo(HELD_DOCS));
            // The size the response arrived with, which includes the source of every hit.
            assertThat(relayBreaker.getUsed(), greaterThanOrEqualTo((long) HELD_DOCS * PAYLOAD_CHARS));

            Exception e = expectThrows(
                CircuitBreakingException.class,
                () -> client(relay).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id))
                    .actionGet(SAFE_AWAIT_TIMEOUT)
                    .decRef()
            );
            assertThat(e.getMessage(), containsString("[" + TransportGetAsyncSearchAction.RELAYED_RESPONSE_BREAKER_LABEL + "]"));
        } finally {
            held.decRef();
        }
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));

        // Once the first response is released, there is room again.
        getThroughRelay(id).decRef();
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));
        deleteAsyncSearch(id);
    }

    /**
     * The node that runs the search charges its serialized reply to a relayed request until it is written, so a reply it has no room
     * for fails the request too. The search is stored, so the owning node reads it from the index first, which holds a charge of twice
     * the stored source until the reply is serialized. The limit leaves room for that read, but not for the reply as well.
     */
    public void testOwnerChargesTheSerializedReply() throws Exception {
        startCluster();
        final String id;
        try (BlockingQueryBuilder.QueryLatch latch = BlockingQueryBuilder.acquireQueryLatch(0)) {
            id = submitBlockingSearch(DOCS);
            latch.close();
            ensureTaskCompletion(id);
        }
        long storedSourceBytes = client(owner).prepareGet(ASYNC_RESULTS_INDEX, AsyncExecutionId.decode(id).getDocId())
            .get()
            .getSourceInternal()
            .length();
        long serializedBytes;
        AsyncSearchResponse direct = client(owner).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            direct.writeTo(out);
            serializedBytes = out.size();
        } finally {
            direct.decRef();
        }

        CircuitBreaker ownerBreaker = internalCluster().getInstance(CircuitBreakerService.class, owner).getBreaker(CircuitBreaker.REQUEST);
        CircuitBreaker relayBreaker = internalCluster().getInstance(CircuitBreakerService.class, relay).getBreaker(CircuitBreaker.REQUEST);
        long ownerTripsBefore = ownerBreaker.getTrippedCount();
        long relayTripsBefore = relayBreaker.getTrippedCount();
        // Applies to the relaying node too, which then has room for the reply.
        updateClusterSettings(Settings.builder().put("indices.breaker.request.limit", (2 * storedSourceBytes + serializedBytes / 2) + "b"));
        try {
            Exception e = expectThrows(
                Exception.class,
                () -> client(relay).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id))
                    .actionGet(SAFE_AWAIT_TIMEOUT)
                    .decRef()
            );
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertNotNull(ExceptionsHelper.unwrap(e, CircuitBreakingException.class));
            assertThat(ownerBreaker.getTrippedCount(), greaterThan(ownerTripsBefore));
            assertThat(relayBreaker.getTrippedCount(), equalTo(relayTripsBefore));
        } finally {
            updateClusterSettings(Settings.builder().putNull("indices.breaker.request.limit"));
        }
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));
        deleteAsyncSearch(id);
    }

    private AsyncSearchResponse getThroughRelay(String id) throws Exception {
        return client(relay).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
    }

    /**
     * Submits a search on the node that runs it, which blocks until the query latch is released.
     */
    private String submitBlockingSearch(int size) throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(
            new SearchSourceBuilder().query(new BlockingQueryBuilder(randomLong())).size(size),
            INDEX
        ).setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        AsyncSearchResponse response = client(owner).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        try {
            assertTrue(response.isRunning());
            return response.getId();
        } finally {
            response.decRef();
        }
    }
}
