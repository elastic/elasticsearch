/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.DISCARD;
import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.INDEX;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.DEST;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public final class IntoMergeStatsOperatorTests extends ComputeTestCase {
    private static final IntoOptions REFRESH = new IntoOptions(true);
    private static final IntoOptions NO_REFRESH = new IntoOptions(false);

    private static final String CALLER_HEADER = "x-caller";

    private final ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
    private final IntoTestUtils.CapturingClient client = new IntoTestUtils.CapturingClient(threadPool);
    private final BlockFactory blockFactory = blockFactory();
    private final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testZeroInputPagesEmitsZeroZero() {
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
        op.finish();
        assertTrue(op.canProduceMoreDataWithoutExtraInput());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(0L, 0L)));
        assertTrue(op.isFinished());
    }

    public void testMultiplePagesAreSummed() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        op.addInput(new IntoStats(3L, 2L).toPage(blockFactory));
        op.addInput(new IntoStats(5L, 4L).toPage(blockFactory));
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(9L, 7L)));
        assertTrue(op.isFinished());
    }

    public void testRefreshesDestinationOnceAfterAllWritersFinish() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(3L, 3L).toPage(blockFactory));
        op.addInput(new IntoStats(2L, 2L).toPage(blockFactory));
        assertThat("no refresh while writers may still report", client.refreshes, empty());

        op.finish();
        assertThat(client.refreshes, hasSize(1));
        assertThat(List.of(client.refreshes.getFirst().indices()), equalTo(List.of(DEST)));
        IsBlockedResult blocked = op.isBlocked();
        assertFalse(blocked.listener().isDone());
        assertFalse(op.canProduceMoreDataWithoutExtraInput());
        assertThat(op.getOutput(), nullValue());
        assertFalse(op.isFinished());
        assertTrue(driverContext.hasPendingAsyncActions());

        client.answerRefresh();
        assertTrue(blocked.listener().isDone());
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(5L, 5L)));
        assertTrue(op.isFinished());
        assertThat(client.refreshes, empty());
    }

    public void testRefreshFalseSkipsRefresh() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        op.addInput(new IntoStats(2L, 2L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(2L, 2L)));
        assertThat(client.refreshes, empty());
    }

    public void testNothingCreatedSkipsRefresh() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(0L, 0L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(0L, 0L)));
        assertThat(client.refreshes, empty());
    }

    public void testDiscardSkipsRefresh() {
        IntoMergeStatsOperator op = operator(REFRESH, DISCARD);
        op.addInput(new IntoStats(2L, 2L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(2L, 2L)));
        assertThat(client.refreshes, empty());
    }

    public void testRefreshFailureFailsTheOperator() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();
        IsBlockedResult blocked = op.isBlocked();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertTrue(blocked.listener().isDone());
        assertFalse(driverContext.hasPendingAsyncActions());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), containsString("INTO [" + DEST + "] wrote [1] documents but the refresh failed"));
        assertThat(e.getCause(), instanceOf(IndexNotFoundException.class));
        assertFalse(op.isFinished());
    }

    public void testForbiddenRefreshExplainsDocumentsWereWritten() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(4L, 4L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new ElasticsearchSecurityException("action [indices:admin/refresh] is unauthorized", RestStatus.FORBIDDEN));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(
            e.getMessage(),
            equalTo(
                "INTO ["
                    + DEST
                    + "] wrote [4] documents but the refresh failed: action [indices:admin/refresh] is unauthorized"
                    + " (refreshing requires the [maintenance] privilege on the destination; use \"refresh\": false to skip it)"
            )
        );
    }

    public void testShardFailuresFailTheRefresh() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        IllegalStateException shardClosed = new IllegalStateException("shard is closed");
        client.answerRefresh(new BroadcastResponse(2, 1, 1, List.of(shardFailure(shardClosed))));
        assertFalse(driverContext.hasPendingAsyncActions());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.getMessage(), containsString("wrote [1] documents but the refresh failed"));
        assertThat(ExceptionsHelper.unwrapCause(e.getCause()), instanceOf(IllegalStateException.class));
    }

    public void testClosedMemberOfAnAliasIsSkippedOnARetry() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();
        assertFalse(
            "the first refresh is strict, so a destination the caller may not refresh is refused, not dropped",
            client.refreshes.getFirst().indicesOptions().ignoreUnavailable()
        );

        client.failRefresh(new IndexClosedException(new Index("older", "uuid")));
        assertThat("retried once", client.refreshes, hasSize(1));
        assertTrue(client.refreshes.getFirst().indicesOptions().ignoreUnavailable());
        assertTrue("the driver still waits for the retry", driverContext.hasPendingAsyncActions());

        client.answerRefresh();
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
    }

    public void testClosedShardDuringTheRefreshIsNotRetried() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        // The write index closed after the refresh had resolved its targets. A retry would skip it and refresh nothing.
        IndexClosedException closed = new IndexClosedException(new Index(DEST, "uuid"));
        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(closed))));
        assertThat(client.refreshes, empty());
        assertFalse(driverContext.hasPendingAsyncActions());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(ExceptionsHelper.unwrapCause(e.getCause()), instanceOf(IndexClosedException.class));
    }

    public void testShardFailureIsReportedWithItsUnderlyingCause() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        IndexClosedException closed = new IndexClosedException(new Index(DEST, "uuid"));
        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(closed))));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.getMessage(), equalTo("INTO [" + DEST + "] wrote [1] documents but the refresh failed: closed"));
        assertThat(e.getCause(), sameInstance(closed));
        // The wrapper said which shard it was, and the error says so itself
        assertThat(e.getShardId(), equalTo(new ShardId(DEST, "uuid", 0)));
    }

    /**
     * A failure that has no index or shard of its own, such as the rejection of a replica, must still say which shard it was in the
     * response, and its root cause must be the error that says the documents were written, not the empty wrapper around it.
     */
    @SuppressWarnings("unchecked")
    public void testSerializedShardFailureHasTheMessageAndTheShard() throws IOException {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(new IllegalStateException("replica rejected")))));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);

        String message = "INTO [" + DEST + "] wrote [1] documents but the refresh failed: replica rejected";
        Map<String, Object> error = errorBody(e);
        assertThat(error.get("reason"), equalTo(message));
        assertThat(error.get("index"), equalTo(DEST));
        assertThat(error.get("shard"), equalTo("0"));
        Map<String, Object> rootCause = ((List<Map<String, Object>>) error.get("root_cause")).getFirst();
        assertThat(rootCause.get("reason"), equalTo(message));
        assertThat(rootCause.get("shard"), equalTo("0"));
    }

    /**
     * The root cause is what actually went wrong, which is how Elasticsearch reports a failure that wraps another one. The message that
     * the documents were written and the shard are on the error itself.
     */
    @SuppressWarnings("unchecked")
    public void testSerializedRootCauseOfAShardFailureIsTheFailureItself() throws IOException {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        CircuitBreakingException breaker = new CircuitBreakingException("[parent] Data too large", CircuitBreaker.Durability.TRANSIENT);
        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(breaker))));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);

        Map<String, Object> error = errorBody(e);
        assertThat(error.get("reason"), equalTo("INTO [" + DEST + "] wrote [1] documents but the refresh failed: [parent] Data too large"));
        assertThat(error.get("index"), equalTo(DEST));
        assertThat(error.get("shard"), equalTo("0"));
        Map<String, Object> rootCause = ((List<Map<String, Object>>) error.get("root_cause")).getFirst();
        assertThat(rootCause.get("type"), equalTo("circuit_breaking_exception"));
        assertThat(rootCause.get("reason"), equalTo("[parent] Data too large"));
    }

    /**
     * Which node refused, and the other details of a remote failure, are on the wrappers, which the message and the cause leave out.
     * The response renders a wrapper as what it wraps, so the details are in the stack trace that {@code error_trace=true} adds.
     */
    public void testErrorTraceKeepsTheTransportDetailsOfAShardFailure() throws IOException {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        CircuitBreakingException breaker = new CircuitBreakingException("[parent] Data too large", CircuitBreaker.Durability.TRANSIENT);
        RemoteTransportException remote = new RemoteTransportException("[node-b][127.0.0.1:9300][indices:admin/refresh[s][r]]", breaker);
        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(remote))));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);

        assertThat(e.getCause(), sameInstance(breaker));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat("the cause chain is already the cause of the error", e.getSuppressed()[0].getCause(), nullValue());
        String stackTrace = (String) errorBody(e, true).get("stack_trace");
        assertThat(stackTrace, containsString("node-b"));
        assertThat(stackTrace, containsString("BroadcastShardOperationFailedException"));
        assertThat(stackTrace, not(containsString("CIRCULAR REFERENCE")));
    }

    /** The wrappers are kept for their details, so what they wrap must not be reported a second time as a suppressed failure. */
    @SuppressWarnings("unchecked")
    public void testSuppressedWrappersDoNotRepeatTheFailure() throws IOException {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        CircuitBreakingException breaker = new CircuitBreakingException("[parent] Data too large", CircuitBreaker.Durability.TRANSIENT);
        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(breaker))));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);

        List<Map<String, Object>> suppressed = (List<Map<String, Object>>) errorBody(e).get("suppressed");
        for (Map<String, Object> entry : suppressed) {
            assertThat(entry.get("type"), not(equalTo("circuit_breaking_exception")));
            assertThat((String) entry.get("reason"), not(containsString("Data too large")));
        }
    }

    public void testRefreshIsOnlyRetriedForAClosedMember() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertThat(client.refreshes, empty());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat("an action-level failure is its own cause, so there is nothing to keep apart", e.getSuppressed(), emptyArray());
    }

    public void testRefreshThatReachedNoShardFails() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        // The primary was not available, which broadcast replication counts in the total but records as neither a success nor a failure
        client.answerRefresh(new BroadcastResponse(2, 0, 0, List.of()));
        assertFalse(driverContext.hasPendingAsyncActions());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(e.getMessage(), containsString("wrote [1] documents but the refresh failed"));
    }

    public void testRefreshOfAnUnderReplicatedDestinationSucceeds() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        // An unassigned replica is in the total but not among the successes, which is normal on a yellow cluster
        client.answerRefresh(new BroadcastResponse(2, 1, 0, List.of()));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
    }

    public void testRefreshRunsWithTheCallersThreadContext() {
        threadPool.getThreadContext().putHeader(CALLER_HEADER, "caller");
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            assertThat(threadPool.getThreadContext().getHeader(CALLER_HEADER), nullValue());
            op.finish();
        }
        assertThat(client.refreshHeaders, equalTo(List.of("caller")));
        client.answerRefresh();
    }

    public void testFinishTwiceRefreshesOnce() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();
        op.finish();
        assertThat(client.refreshes, hasSize(1));
        client.answerRefresh();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
    }

    /** The body of the error response, as the REST layer renders it. */
    private static Map<String, Object> errorBody(Exception e) throws IOException {
        return errorBody(e, false);
    }

    /** The body of the error response, with the stack trace that {@code error_trace=true} adds if asked for. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> errorBody(Exception e, boolean errorTrace) throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(
            Map.of(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, Boolean.toString(errorTrace == false))
        );
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            ElasticsearchException.generateFailureXContent(builder, params, e, true);
            builder.endObject();
            return (Map<String, Object>) XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON)
                .v2()
                .get("error");
        }
    }

    /** A shard failure as broadcast replication reports it: wrapped in an exception that has no message of its own. */
    private static DefaultShardOperationFailedException shardFailure(Throwable cause) {
        return new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(new ShardId(DEST, "uuid", 0), cause));
    }

    private IntoMergeStatsOperator operator(IntoOptions options, IntoPageWriter.Factory.Indexing indexing) {
        return (IntoMergeStatsOperator) IntoMergeStatsOperator.Factory.create(client, DEST, options, indexing).get(driverContext);
    }
}
