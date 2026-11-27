/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.DISCARD;
import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.INDEX;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.DEST;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.noFailures;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public final class IntoMergeStatsOperatorTests extends ComputeTestCase {
    private static final IntoOptions REFRESH = IntoOptions.builder().refresh(true).build();
    private static final IntoOptions NO_REFRESH = IntoOptions.builder().refresh(false).build();

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
        assertThat(stats(op.getOutput()), equalTo(noFailures(0L, 0L)));
        assertTrue(op.isFinished());
    }

    public void testMultiplePagesAreSummed() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        op.addInput(new IntoStats(3L, 2L, 1L, 0L, 0L, List.of()).toPage(blockFactory));
        op.addInput(new IntoStats(5L, 1L, 0L, 4L, 0L, List.of()).toPage(blockFactory));
        op.addInput(new IntoStats(1L, 1L, 0L, 0L, 0L, List.of()).toPage(blockFactory));
        op.finish();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(9L, 4L, 1L, 4L, 0L, List.of())));
        assertTrue(op.isFinished());
    }

    public void testRefreshesDestinationOnceAfterAllWritersFinish() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(3L, 3L).toPage(blockFactory));
        op.addInput(noFailures(2L, 2L).toPage(blockFactory));
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
        assertThat(stats(op.getOutput()), equalTo(noFailures(5L, 5L)));
        assertTrue(op.isFinished());
        assertThat(client.refreshes, empty());
    }

    public void testRefreshFalseSkipsRefresh() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        op.addInput(noFailures(2L, 2L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(noFailures(2L, 2L)));
        assertThat(client.refreshes, empty());
    }

    public void testNothingWrittenSkipsRefresh() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(0L, 0L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(noFailures(0L, 0L)));
        assertThat(client.refreshes, empty());
    }

    public void testDiscardSkipsRefresh() {
        IntoMergeStatsOperator op = operator(REFRESH, DISCARD);
        op.addInput(noFailures(2L, 2L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(noFailures(2L, 2L)));
        assertThat(client.refreshes, empty());
    }

    private static final String FAILED = "index refresh for INTO ["
        + DEST
        + "] failed after [1] documents were written, so they may not be searchable yet: ";

    /**
     * The rows are stored whatever the refresh does, and a failed query invites a retry that stores them again, so a refresh that
     * fails is a warning on the stats row, never a failure.
     */
    public void testRefreshFailureIsAWarningAndTheStatsAreStillEmitted() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();
        IsBlockedResult blocked = op.isBlocked();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertTrue(blocked.listener().isDone());
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        assertTrue(op.isFinished());
        assertThat(warnings(), equalTo(List.of(FAILED + "no such index [" + DEST + "]")));
    }

    public void testForbiddenRefreshWarningExplainsThePrivilege() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(4L, 4L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new ElasticsearchSecurityException("action [indices:admin/refresh] is unauthorized", RestStatus.FORBIDDEN));
        assertThat(stats(op.getOutput()), equalTo(noFailures(4L, 4L)));
        assertThat(
            warnings(),
            equalTo(
                List.of(
                    "index refresh for INTO ["
                        + DEST
                        + "] failed after [4] documents were written, so they may not be searchable yet: "
                        + "action [indices:admin/refresh] is unauthorized"
                        + " (INTO includes an index refresh that requires the [maintenance] privilege; use \"refresh\": false to skip it)"
                )
            )
        );
    }

    public void testShardFailureIsAWarningWithItsUnderlyingMessage() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        // The wrapper that broadcast replication puts around a shard failure has no message of its own
        client.answerRefresh(new BroadcastResponse(2, 1, 1, List.of(shardFailure(new IllegalStateException("shard is closed")))));
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(FAILED + "shard is closed")));
    }

    public void testClosedMemberOfAnAliasIsSkippedOnARetry() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
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
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        // It cannot tell which closed member the write went to, or whether one was skipped at all, so the caller is told that it may be
        assertThat(
            warnings(),
            equalTo(
                List.of(
                    "index refresh for INTO ["
                        + DEST
                        + "] may have skipped closed indices of the destination, so the documents may not be searchable yet"
                )
            )
        );
    }

    public void testClosedShardDuringTheRefreshIsNotRetried() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        // The write index closed after the refresh had resolved its targets. A retry would skip it and refresh nothing.
        IndexClosedException closed = new IndexClosedException(new Index(DEST, "uuid"));
        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(closed))));
        assertThat(client.refreshes, empty());
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(FAILED + "closed")));
    }

    public void testRefreshIsOnlyRetriedForAClosedMember() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertThat(client.refreshes, empty());
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
    }

    public void testRefreshThatReachedNoShardIsAWarning() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        // The primary was not available, which broadcast replication counts in the total but records as neither a success nor a failure
        client.answerRefresh(new BroadcastResponse(2, 0, 0, List.of()));
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(FAILED + "no shard of the destination was refreshed")));
    }

    public void testRefreshOfAnUnderReplicatedDestinationSucceeds() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        // An unassigned replica is in the total but not among the successes, which is normal on a yellow cluster
        client.answerRefresh(new BroadcastResponse(2, 1, 0, List.of()));
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        assertThat(warnings(), empty());
    }

    /**
     * Which node refused, and its transport address, are internal details: the warning, which every caller receives, must not carry
     * them.
     */
    public void testWarningDoesNotDiscloseTheRemoteNode() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        client.answerRefresh(new BroadcastResponse(1, 0, 1, List.of(shardFailure(remoteBreaker()))));
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
        assertThat(warnings(), equalTo(List.of(FAILED + "[parent] Data too large")));
    }

    /**
     * A cancelled query finishes the driver context while the refresh is still in flight, and the failure of that refresh then arrives
     * on a transport thread. It must not write into the finished context, which asserts that nobody does.
     */
    public void testRefreshFailureAfterTheDriverContextFinishedIsDropped() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();

        driverContext.finish();
        client.failRefresh(new IndexNotFoundException(DEST));

        assertThat(driverContext.warnings(), empty());
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
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            assertThat(threadPool.getThreadContext().getHeader(CALLER_HEADER), nullValue());
            op.finish();
        }
        assertThat(client.refreshHeaders, equalTo(List.of("caller")));
        client.answerRefresh();
    }

    public void testFinishTwiceRefreshesOnce() {
        IntoMergeStatsOperator op = operator(REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();
        op.finish();
        assertThat(client.refreshes, hasSize(1));
        client.answerRefresh();
        assertThat(stats(op.getOutput()), equalTo(noFailures(1L, 1L)));
    }

    public void testFailuresAreSummedAndDetailsConcatenatedInArrivalOrder() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        op.addInput(new IntoStats(4L, 2L, 0L, 0L, 2L, List.of("a", "b")).toPage(blockFactory));
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.addInput(new IntoStats(5L, 2L, 0L, 0L, 3L, List.of("c", "d", "e")).toPage(blockFactory));
        op.finish();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(10L, 5L, 0L, 0L, 5L, List.of("a", "b", "c", "d", "e"))));
    }

    public void testFailureDetailsAreCappedAfterMerge() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        List<String> first = details("first", 15);
        List<String> second = details("second", 15);
        op.addInput(new IntoStats(15L, 0L, 0L, 0L, 15L, first).toPage(blockFactory));
        op.addInput(new IntoStats(15L, 0L, 0L, 0L, 15L, second).toPage(blockFactory));
        op.finish();

        List<String> expected = new ArrayList<>(first);
        expected.addAll(second.subList(0, IntoStats.MAX_FAILURE_DETAILS - first.size()));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(30L, 0L, 0L, 0L, 30L, expected)));
    }

    public void testNoFailureDetailsIsANullValue() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        op.addInput(noFailures(1L, 1L).toPage(blockFactory));
        op.finish();
        Page output = op.getOutput();
        try {
            BytesRefBlock details = output.getBlock(5);
            assertTrue(details.isNull(0));
        } finally {
            output.releaseBlocks();
        }
    }

    public void testKeepGoingRefreshesWhenSomeRowsWereCreated() {
        IntoMergeStatsOperator op = operator(IntoOptions.builder().refresh(true).keepGoing(true).build(), INDEX);
        op.addInput(new IntoStats(3L, 1L, 0L, 0L, 2L, List.of("a", "b")).toPage(blockFactory));
        op.finish();
        assertThat(client.refreshes, hasSize(1));
        assertThat(op.getOutput(), nullValue());

        client.answerRefresh();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(3L, 1L, 0L, 0L, 2L, List.of("a", "b"))));
        assertTrue(op.isFinished());
    }

    public void testKeepGoingSkipsRefreshWhenEveryRowWasRefused() {
        IntoMergeStatsOperator op = operator(IntoOptions.builder().refresh(true).keepGoing(true).build(), INDEX);
        op.addInput(new IntoStats(2L, 0L, 0L, 0L, 2L, List.of("a", "b")).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(2L, 0L, 0L, 0L, 2L, List.of("a", "b"))));
        assertThat(client.refreshes, empty());
    }

    public void testOnlyVersionConflictsSkipsRefresh() {
        IntoMergeStatsOperator op = operator(IntoOptions.builder().refresh(true).collisions(IntoOptions.Collisions.KEEP).build(), INDEX);
        op.addInput(new IntoStats(3L, 0L, 0L, 3L, 0L, List.of()).toPage(blockFactory));
        op.addInput(new IntoStats(2L, 0L, 0L, 2L, 0L, List.of()).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(5L, 0L, 0L, 5L, 0L, List.of())));
        assertThat("kept rows changed nothing, so there is nothing to make visible", client.refreshes, empty());
    }

    public void testOnlyUpdatesRefreshes() {
        IntoMergeStatsOperator op = operator(IntoOptions.builder().refresh(true).collisions(IntoOptions.Collisions.REPLACE).build(), INDEX);
        op.addInput(new IntoStats(2L, 0L, 2L, 0L, 0L, List.of()).toPage(blockFactory));
        op.finish();
        assertThat("replaced documents must become visible too", client.refreshes, hasSize(1));
        assertFalse(op.canProduceMoreDataWithoutExtraInput());

        client.answerRefresh();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(2L, 0L, 2L, 0L, 0L, List.of())));
        assertTrue(op.isFinished());
    }

    public void testRefreshFailureCountsUpdatedDocumentsAsWritten() {
        IntoMergeStatsOperator op = operator(IntoOptions.builder().refresh(true).collisions(IntoOptions.Collisions.REPLACE).build(), INDEX);
        op.addInput(new IntoStats(3L, 1L, 2L, 0L, 0L, List.of()).toPage(blockFactory));
        op.finish();

        client.failRefresh(new IndexNotFoundException(DEST));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(3L, 1L, 2L, 0L, 0L, List.of())));
        String format = "index refresh for INTO [%s] failed after [%d] documents were written, so they may not be searchable yet: %s";
        assertThat(warnings(), equalTo(List.of(Strings.format(format, DEST, 3, "no such index [" + DEST + "]"))));
    }

    public void testEveryPositionOfAPageIsSummed() {
        IntoMergeStatsOperator op = operator(NO_REFRESH, INDEX);
        Page first = new IntoStats(3L, 1L, 1L, 1L, 0L, List.of()).toPage(blockFactory);
        Page second = new IntoStats(2L, 0L, 2L, 0L, 0L, List.of()).toPage(blockFactory);
        Block[] blocks = new Block[first.getBlockCount()];
        int detailsChannel = blocks.length - 1;
        for (int b = 0; b < detailsChannel; b++) {
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(2)) {
                builder.appendLong(first.<LongBlock>getBlock(b).getLong(0));
                builder.appendLong(second.<LongBlock>getBlock(b).getLong(0));
                blocks[b] = builder.build();
            }
        }
        // Neither of the rows has failure details
        blocks[detailsChannel] = blockFactory.newConstantNullBlock(2);
        first.releaseBlocks();
        second.releaseBlocks();
        op.addInput(new Page(blocks));
        op.finish();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(5L, 1L, 3L, 1L, 0L, List.of())));
    }

    public void testStatsRoundTripThroughAPage() {
        IntoStats original = new IntoStats(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomList(0, 3, () -> randomAlphaOfLength(5))
        );
        assertThat(stats(original.toPage(blockFactory)), equalTo(original));
    }

    public void testMergeAddsEveryCounter() {
        assertThat(
            new IntoStats(1, 2, 3, 4, 0L, List.of()).merge(new IntoStats(10, 20, 30, 40, 0L, List.of())),
            equalTo(new IntoStats(11, 22, 33, 44, 0L, List.of()))
        );
        assertThat(IntoStats.EMPTY.merge(new IntoStats(1, 2, 3, 4, 0L, List.of())), equalTo(new IntoStats(1, 2, 3, 4, 0L, List.of())));
    }

    public void testAttributesMatchThePageLayout() {
        List<Attribute> attributes = IntoStats.createAttributes(Source.EMPTY);
        assertThat(
            Expressions.names(attributes),
            equalTo(List.of("total", "created", "updated", "version_conflicts", "failures", "failure_details"))
        );
        assertThat(
            attributes.stream().map(Attribute::dataType).toList(),
            equalTo(List.of(DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG, DataType.KEYWORD))
        );
        Page page = IntoStats.EMPTY.toPage(blockFactory);
        try {
            assertThat(page.getBlockCount(), equalTo(attributes.size()));
        } finally {
            page.releaseBlocks();
        }
    }

    /** A shard failure as broadcast replication reports it: wrapped in an exception that has no message of its own. */
    private static DefaultShardOperationFailedException shardFailure(Throwable cause) {
        return new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(new ShardId(DEST, "uuid", 0), cause));
    }

    private IntoMergeStatsOperator operator(IntoOptions options, IntoPageWriter.Factory.Indexing indexing) {
        return (IntoMergeStatsOperator) IntoMergeStatsOperator.Factory.create(client, DEST, options, indexing).get(driverContext);
    }

    private static List<String> details(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(i -> prefix + "-" + i).toList();
    }
}
