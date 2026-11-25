/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.After;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.DISCARD;
import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.INDEX;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.DEST;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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

    private static final String WRITTEN = "INTO ["
        + DEST
        + "] wrote [1] documents successfully, only the refresh failed, so they may not be searchable yet: ";

    /**
     * The rows are stored whatever the refresh does, and a failed query invites a retry that stores them again, so a refresh that
     * fails is a warning on the stats row, never a failure.
     */
    public void testRefreshFailureIsAWarningAndTheStatsAreStillEmitted() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();
        IsBlockedResult blocked = op.isBlocked();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertTrue(blocked.listener().isDone());
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertTrue(op.isFinished());
        assertThat(warnings(), equalTo(List.of(WRITTEN + "no such index [" + DEST + "]")));
    }

    public void testForbiddenRefreshWarningExplainsThePrivilege() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(4L, 4L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new ElasticsearchSecurityException("action [indices:admin/refresh] is unauthorized", RestStatus.FORBIDDEN));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(4L, 4L)));
        assertThat(
            warnings(),
            equalTo(
                List.of(
                    "INTO ["
                        + DEST
                        + "] wrote [4] documents successfully, only the refresh failed, so they may not be searchable yet: "
                        + "action [indices:admin/refresh] is unauthorized"
                        + " (refreshing requires the [maintenance] privilege on the destination; use \"refresh\": false to skip it)"
                )
            )
        );
    }

    public void testShardFailureIsAWarningWithItsUnderlyingMessage() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        // The wrapper that broadcast replication puts around a shard failure has no message of its own
        client.answerRefresh(new BroadcastResponse(2, 1, 1, List.of(shardFailure(new IllegalStateException("shard is closed")))));
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(WRITTEN + "shard is closed")));
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
        assertThat(warnings(), empty());
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
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(WRITTEN + "closed")));
    }

    public void testRefreshIsOnlyRetriedForAClosedMember() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertThat(client.refreshes, empty());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
    }

    public void testRefreshThatReachedNoShardIsAWarning() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        // The primary was not available, which broadcast replication counts in the total but records as neither a success nor a failure
        client.answerRefresh(new BroadcastResponse(2, 0, 0, List.of()));
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(WRITTEN + "no shard of the destination was refreshed")));
    }

    public void testRefreshOfAnUnderReplicatedDestinationSucceeds() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        // An unassigned replica is in the total but not among the successes, which is normal on a yellow cluster
        client.answerRefresh(new BroadcastResponse(2, 1, 0, List.of()));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertThat(warnings(), empty());
    }

    /**
     * Which node refused, and its transport address, are internal details: the warning, which every caller receives, must not carry
     * them. The cluster admin gets them from the log instead.
     */
    public void testWarningDoesNotDiscloseTheRemoteNode() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(remoteBreaker()))));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(WRITTEN + "[parent] Data too large")));
    }

    /** The node that refused is what the cluster admin needs, so it goes to the log, with the whole failure. */
    public void testRemoteNodeOfAShardFailureIsLogged() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        try (MockLog mockLog = MockLog.capture(IntoMergeStatsOperator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "refresh failure names the node",
                    IntoMergeStatsOperator.class.getCanonicalName(),
                    Level.WARN,
                    "*[" + DEST + "]*node-b*"
                )
            );
            client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(remoteBreaker()))));
            assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
            mockLog.assertAllExpectationsMatched();
        }
    }

    /** A cause chain can be cyclic, and describing it for the log must still end, or the refresh never settles. */
    public void testCyclicCauseChainDoesNotBlockTheOperator() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        Throwable[] other = new Throwable[1];
        AtomicInteger calls = new AtomicInteger();
        RuntimeException first = new RuntimeException("first") {
            @Override
            public synchronized Throwable getCause() {
                // Fail promptly if the walk does not end, instead of exhausting the heap of the test JVM
                if (calls.incrementAndGet() > 1_000) {
                    throw new AssertionError("the cause chain is walked without end");
                }
                return other[0];
            }
        };
        other[0] = new RuntimeException("second", first);
        client.failRefresh(new ElasticsearchException("refresh failed", first));

        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(WRITTEN + "refresh failed")));
    }

    private static RemoteTransportException remoteBreaker() {
        CircuitBreakingException breaker = new CircuitBreakingException("[parent] Data too large", CircuitBreaker.Durability.TRANSIENT);
        return new RemoteTransportException("[node-b][127.0.0.1:9300][indices:admin/refresh[s][r]]", breaker);
    }

    private List<String> warnings() {
        driverContext.finish();
        return driverContext.warnings();
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

    /** A shard failure as broadcast replication reports it: wrapped in an exception that has no message of its own. */
    private static DefaultShardOperationFailedException shardFailure(Throwable cause) {
        return new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(new ShardId(DEST, "uuid", 0), cause));
    }

    private IntoMergeStatsOperator operator(IntoOptions options, IntoPageWriter.Factory.Indexing indexing) {
        return (IntoMergeStatsOperator) IntoMergeStatsOperator.Factory.create(client, DEST, options, indexing).get(driverContext);
    }
}
