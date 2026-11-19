/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.DISCARD;
import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.INDEX;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.DEST;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IntoMergeStatsOperatorTests extends ComputeTestCase {
    private static final IntoOptions NO_REFRESH = new IntoOptions(false);

    private static final String CALLER_HEADER = "x-caller";

    private ThreadPool threadPool;
    private IntoTestUtils.CapturingClient client;
    private BlockFactory blockFactory;
    private DriverContext driverContext;

    @Before
    public void setUpClient() {
        threadPool = new TestThreadPool(getTestName());
        client = new IntoTestUtils.CapturingClient(threadPool);
        blockFactory = blockFactory();
        driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    @After
    public void stopThreadPool() {
        if (threadPool != null) {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
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
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
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
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
        op.addInput(new IntoStats(0L, 0L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(0L, 0L)));
        assertThat(client.refreshes, empty());
    }

    public void testDiscardSkipsRefresh() {
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, DISCARD);
        op.addInput(new IntoStats(2L, 2L).toPage(blockFactory));
        op.finish();
        assertThat(op.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(2L, 2L)));
        assertThat(client.refreshes, empty());
    }

    public void testRefreshFailureFailsTheOperator() {
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
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
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
        op.addInput(new IntoStats(4L, 4L).toPage(blockFactory));
        op.finish();

        client.failRefresh(new ElasticsearchSecurityException("action [indices:admin/refresh] is unauthorized", RestStatus.FORBIDDEN));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("wrote [4] documents"));
        assertThat(e.getMessage(), containsString("[maintenance] privilege"));
        assertThat(e.getMessage(), containsString("\"refresh\": false"));
    }

    public void testShardFailuresFailTheRefresh() {
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();

        client.answerRefresh(
            new BroadcastResponse(
                2,
                1,
                1,
                List.of(new DefaultShardOperationFailedException(DEST, 0, new IllegalStateException("shard is closed")))
            )
        );
        assertFalse(driverContext.hasPendingAsyncActions());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.getMessage(), containsString("wrote [1] documents but the refresh failed"));
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));
    }

    public void testRefreshRunsWithTheCallersThreadContext() {
        threadPool.getThreadContext().putHeader(CALLER_HEADER, "caller");
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            assertThat(threadPool.getThreadContext().getHeader(CALLER_HEADER), nullValue());
            op.finish();
        }
        assertThat(client.refreshHeaders, equalTo(List.of("caller")));
        client.answerRefresh();
    }

    public void testFinishTwiceRefreshesOnce() {
        IntoMergeStatsOperator op = operator(IntoOptions.DEFAULT, INDEX);
        op.addInput(new IntoStats(1L, 1L).toPage(blockFactory));
        op.finish();
        op.finish();
        assertThat(client.refreshes, hasSize(1));
        client.answerRefresh();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(1L, 1L)));
    }

    private IntoMergeStatsOperator operator(IntoOptions options, IntoPageWriter.Factory.Indexing indexing) {
        return (IntoMergeStatsOperator) IntoMergeStatsOperator.Factory.create(client, DEST, options, indexing).get(driverContext);
    }
}
