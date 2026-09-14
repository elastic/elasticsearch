/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeResponse;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.RemoteSink;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class ComputeServiceLocalSourceProducerTests extends ComputeTestCase {
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";

    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 8);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, ESQL_TEST_EXECUTOR, numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testComputeThenFetchSuccessPublishesOnce() throws Exception {
        try (ProducerHarness harness = newProducer(randomBoolean())) {
            harness.computeListener().onResponse(completionInfo(7, false));
            assertFalse(harness.future.isDone());
            harness.succeedFetch();
            DriverCompletionInfo info = awaitSuccess(harness);
            assertThat(info.rowsEmitted(), equalTo(7L));
            assertFalse(info.partial());
            assertTrue(harness.outcomes.externalSourceSucceeded());
            assertThat(harness.outcomes.externalSourceFailures(), empty());
            assertThat(harness.terminals.get(), equalTo(1));
        }
    }

    public void testFetchThenComputeSuccessPublishesOnce() throws Exception {
        try (ProducerHarness harness = newProducer(randomBoolean())) {
            harness.succeedFetch();
            assertFalse(harness.future.isDone());
            harness.computeListener().onResponse(completionInfo(4, false));
            DriverCompletionInfo info = awaitSuccess(harness);
            assertThat(info.rowsEmitted(), equalTo(4L));
            assertTrue(harness.outcomes.externalSourceSucceeded());
            assertThat(harness.terminals.get(), equalTo(1));
        }
    }

    public void testEmptyProducerSuccessIsStillSuccess() throws Exception {
        try (ProducerHarness harness = newProducer(randomBoolean())) {
            harness.computeListener().onResponse(DriverCompletionInfo.EMPTY);
            harness.succeedFetch();
            DriverCompletionInfo info = awaitSuccess(harness);
            assertThat(info, equalTo(DriverCompletionInfo.EMPTY));
            assertTrue(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testReaderLenientPartialDoesNotFailACompletedProducer() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            DriverCompletionInfo compute = completionInfo(3, false).withPartial();
            harness.computeListener().onResponse(compute);
            harness.succeedFetch();
            DriverCompletionInfo info = awaitSuccess(harness);
            assertTrue(info.partial());
            assertThat(info.rowsEmitted(), equalTo(3L));
            assertTrue(harness.outcomes.externalSourceSucceeded());
            assertThat(harness.outcomes.externalSourceFailures(), empty());
        }
    }

    public void testToleratedComputeFailureThenSuccessfulSinkDrainIsPartial() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            IllegalStateException computeFailure = new IllegalStateException("compute failed");
            harness.computeListener().onFailure(computeFailure);
            assertFalse(harness.future.isDone());
            harness.succeedFetch();
            DriverCompletionInfo info = awaitSuccess(harness);
            assertTrue(info.partial());
            assertThat(info.warnings(), hasItem(containsString("compute failed")));
            assertFalse(harness.outcomes.externalSourceSucceeded());
            assertThat(harness.outcomes.externalSourceFailures(), hasSize(1));
            assertThat(harness.outcomes.externalSourceFailures().get(0).getMessage(), equalTo("compute failed"));
        }
    }

    public void testSuccessfulSinkDrainThenToleratedComputeFailureIsPartial() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.succeedFetch();
            assertFalse(harness.future.isDone());
            harness.computeListener().onFailure(new IllegalStateException("compute failed later"));
            DriverCompletionInfo info = awaitSuccess(harness);
            assertTrue(info.partial());
            assertFalse(harness.outcomes.externalSourceSucceeded());
            assertThat(info.warnings(), hasItem(containsString("compute failed later")));
        }
    }

    public void testComputeSuccessThenFetchFailureIsPartialAndKeepsComputeMetrics() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.computeListener().onResponse(completionInfo(9, false));
            assertFalse(harness.future.isDone());
            harness.failFetch(new IllegalStateException("fetch failed"));
            DriverCompletionInfo info = awaitSuccess(harness);
            assertTrue(info.partial());
            assertThat(info.rowsEmitted(), equalTo(9L));
            assertThat(info.warnings(), hasItem(containsString("fetch failed")));
            assertFalse(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testFetchFailureThenComputeSuccessIsPartialAndKeepsComputeMetrics() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.failFetch(new IllegalStateException("fetch failed first"));
            assertFalse(harness.future.isDone());
            harness.computeListener().onResponse(completionInfo(5, false));
            DriverCompletionInfo info = awaitSuccess(harness);
            assertTrue(info.partial());
            assertThat(info.rowsEmitted(), equalTo(5L));
            assertThat(info.warnings(), hasItem(containsString("fetch failed first")));
            assertFalse(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testBothToleratedFailuresKeepBothCauses() throws Exception {
        boolean computeFirst = randomBoolean();
        try (ProducerHarness harness = newProducer(false)) {
            IllegalStateException computeFailure = new IllegalStateException("compute boom");
            IllegalStateException fetchFailure = new IllegalStateException("fetch boom");
            if (computeFirst) {
                harness.computeListener().onFailure(computeFailure);
                harness.failFetch(fetchFailure);
            } else {
                harness.failFetch(fetchFailure);
                harness.computeListener().onFailure(computeFailure);
            }
            DriverCompletionInfo info = awaitSuccess(harness);
            assertTrue(info.partial());
            assertFalse(harness.outcomes.externalSourceSucceeded());
            assertThat(harness.outcomes.externalSourceFailures(), hasSize(2));
            assertThat(info.warnings(), hasItem(containsString("compute boom")));
            assertThat(info.warnings(), hasItem(containsString("fetch boom")));
            assertThat(harness.terminals.get(), equalTo(1));
        }
    }

    public void testFailFastComputeFailureDoesNotWaitForFetch() throws Exception {
        try (ProducerHarness harness = newProducer(true)) {
            ElasticsearchException computeFailure = new ElasticsearchException("fail-fast compute");
            harness.computeListener().onFailure(computeFailure);
            Exception failure = awaitFailure(harness);
            assertThat(ExceptionsHelper.unwrapCause(failure).getMessage(), equalTo("fail-fast compute"));
            assertFalse(harness.outcomes.externalSourceSucceeded());
            assertThat(harness.terminals.get(), equalTo(1));
        }
    }

    public void testFailFastFetchFailureDoesNotWaitForCompute() throws Exception {
        try (ProducerHarness harness = newProducer(true)) {
            harness.failFetch(new ElasticsearchException("fail-fast fetch"));
            Exception failure = awaitFailure(harness);
            assertThat(ExceptionsHelper.unwrapCause(failure).getMessage(), equalTo("fail-fast fetch"));
            harness.computeListener().onResponse(completionInfo(1, false));
            assertThat(harness.terminals.get(), equalTo(1));
            assertFalse(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testCancellationIsFatalEvenWithPartialResults() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.computeListener().onFailure(new TaskCancelledException("cancelled"));
            Exception failure = awaitFailure(harness);
            assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(TaskCancelledException.class));
            harness.succeedFetch();
            assertThat(harness.terminals.get(), equalTo(1));
            assertFalse(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testSecurityRefusalOnFetchIsFatalWithPartialResults() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.failFetch(new ElasticsearchSecurityException("denied"));
            Exception failure = awaitFailure(harness);
            assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(ElasticsearchSecurityException.class));
            harness.computeListener().onResponse(completionInfo(2, false));
            assertThat(harness.terminals.get(), equalTo(1));
            assertFalse(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testMissingIndexOnComputeIsFatalWithPartialResults() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.computeListener().onFailure(new IndexNotFoundException("gone"));
            Exception failure = awaitFailure(harness);
            assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(IndexNotFoundException.class));
            harness.succeedFetch();
            assertThat(harness.terminals.get(), equalTo(1));
            assertFalse(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testLateCallbacksAfterSuccessDoNotRepublish() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.computeListener().onResponse(completionInfo(1, false));
            harness.succeedFetch();
            awaitSuccess(harness);
            harness.computeListener().onFailure(new IllegalStateException("late compute"));
            harness.failFetch(new IllegalStateException("late fetch"));
            assertThat(harness.terminals.get(), equalTo(1));
            assertTrue(harness.outcomes.externalSourceSucceeded());
        }
    }

    public void testEmptySinkLeaseKeepsSourceOpenUntilReleased() throws Exception {
        try (ProducerHarness harness = newProducer(false)) {
            harness.computeListener().onResponse(DriverCompletionInfo.EMPTY);
            harness.succeedFetch();
            awaitSuccess(harness);
            assertFalse(harness.source.isFinished());
            harness.releaseLease();
            assertBusy(() -> assertTrue(harness.source.isFinished()));
        }
    }

    public void testCancelledQueuedProducerDoesNotStart() {
        CancellableTask task = newTask();
        TaskCancelHelper.cancel(task, "cancelled while queued");
        AtomicBoolean started = new AtomicBoolean();
        PlainActionFuture<DriverCompletionInfo> future = new PlainActionFuture<>();
        ComputeService.startOrSkipQueuedSourceProducer(task, executionInfo(), newSource(), future, () -> started.set(true));
        assertFalse(started.get());
        Exception failure = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(TaskCancelledException.class));
    }

    public void testStoppedQueuedProducerCompletesEmptyWithoutStarting() {
        EsqlExecutionInfo execInfo = executionInfo();
        execInfo.markAsStopped();
        AtomicBoolean started = new AtomicBoolean();
        PlainActionFuture<DriverCompletionInfo> future = new PlainActionFuture<>();
        ComputeService.startOrSkipQueuedSourceProducer(newTask(), execInfo, newSource(), future, () -> started.set(true));
        assertFalse(started.get());
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(DriverCompletionInfo.EMPTY));
    }

    public void testFinishedExchangeSkipsQueuedProducer() throws Exception {
        ExchangeSourceHandler source = newSource();
        source.addEmptySink().close();
        assertBusy(() -> assertTrue(source.isFinished()));
        AtomicBoolean started = new AtomicBoolean();
        PlainActionFuture<DriverCompletionInfo> future = new PlainActionFuture<>();
        ComputeService.startOrSkipQueuedSourceProducer(newTask(), executionInfo(), source, future, () -> started.set(true));
        assertFalse(started.get());
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(DriverCompletionInfo.EMPTY));
    }

    public void testQueuedProducerStartsWhenStillRunnable() {
        AtomicBoolean started = new AtomicBoolean();
        PlainActionFuture<DriverCompletionInfo> future = new PlainActionFuture<>();
        ComputeService.startOrSkipQueuedSourceProducer(newTask(), executionInfo(), newSource(), future, () -> {
            started.set(true);
            future.onResponse(DriverCompletionInfo.EMPTY);
        });
        assertTrue(started.get());
        assertThat(future.actionGet(10, TimeUnit.SECONDS), equalTo(DriverCompletionInfo.EMPTY));
    }

    private ProducerHarness newProducer(boolean failFast) throws Exception {
        ProducerHarness harness = new ProducerHarness(failFast);
        assertBusy(() -> assertThat(harness.remote.heldCount(), greaterThan(0)));
        return harness;
    }

    private ExchangeSourceHandler newSource() {
        return new ExchangeSourceHandler(2, threadPool.executor(ESQL_TEST_EXECUTOR));
    }

    private static CancellableTask newTask() {
        return new CancellableTask(1, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static EsqlExecutionInfo executionInfo() {
        return new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
    }

    private static DriverCompletionInfo completionInfo(long rowsEmitted, boolean partial) {
        return new DriverCompletionInfo(0, 0, rowsEmitted, 0, 0, 0, 0, List.of(), List.of(), Map.of(), partial, false, Set.of());
    }

    private static DriverCompletionInfo awaitSuccess(ProducerHarness harness) {
        return harness.future.actionGet(10, TimeUnit.SECONDS);
    }

    private static Exception awaitFailure(ProducerHarness harness) {
        try {
            harness.future.actionGet(10, TimeUnit.SECONDS);
            throw new AssertionError("expected producer failure");
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            return e;
        }
    }

    private final class ProducerHarness implements Releasable {
        private final ExchangeSinkHandler sinkHandler;
        private final ExchangeSink sink;
        private final ExchangeSourceHandler source;
        private final ControllableRemoteSink remote;
        private final SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        private final AtomicInteger terminals = new AtomicInteger();
        private final PlainActionFuture<DriverCompletionInfo> future = new PlainActionFuture<>();
        private final Releasable lease;
        private final LocalSourceProducerLifecycle lifecycle;
        private final AtomicBoolean leaseClosed = new AtomicBoolean();

        private ProducerHarness(boolean failFast) {
            sinkHandler = new ExchangeSinkHandler(blockFactory(), 2, threadPool.relativeTimeInMillisSupplier());
            sink = sinkHandler.createExchangeSink(() -> {});
            source = newSource();
            lease = source.addEmptySink();
            remote = new ControllableRemoteSink(sinkHandler::fetchPageAsync);
            ActionListener<DriverCompletionInfo> listener = ActionListener.wrap(info -> {
                terminals.incrementAndGet();
                future.onResponse(info);
            }, e -> {
                terminals.incrementAndGet();
                future.onFailure(e);
            });
            lifecycle = LocalSourceProducerLifecycle.register(source, remote, failFast, outcomes, listener);
        }

        ActionListener<DriverCompletionInfo> computeListener() {
            return lifecycle.computeListener();
        }

        void succeedFetch() {
            if (sink.isFinished() == false) {
                sink.finish();
            }
            remote.releaseToDelegate();
        }

        void failFetch(Exception failure) {
            remote.failHeld(failure);
        }

        void releaseLease() {
            if (leaseClosed.compareAndSet(false, true)) {
                lease.close();
            }
        }

        @Override
        public void close() {
            if (sink.isFinished() == false) {
                sink.finish();
            }
            remote.releaseToDelegate();
            releaseLease();
        }
    }

    private static final class ControllableRemoteSink implements RemoteSink {
        private final RemoteSink delegate;
        private final Queue<HeldFetch> held = ConcurrentCollections.newQueue();
        private volatile boolean holding = true;

        private record HeldFetch(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener) {}

        private ControllableRemoteSink(RemoteSink delegate) {
            this.delegate = delegate;
        }

        @Override
        public void fetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener) {
            if (holding) {
                held.add(new HeldFetch(allSourcesFinished, listener));
                return;
            }
            delegate.fetchPageAsync(allSourcesFinished, listener);
        }

        int heldCount() {
            return held.size();
        }

        void releaseToDelegate() {
            holding = false;
            HeldFetch fetch;
            while ((fetch = held.poll()) != null) {
                delegate.fetchPageAsync(fetch.allSourcesFinished(), fetch.listener());
            }
        }

        void failHeld(Exception failure) {
            holding = false;
            HeldFetch fetch;
            while ((fetch = held.poll()) != null) {
                fetch.listener().onFailure(failure);
            }
        }
    }
}
