/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReindexerTests extends ESTestCase {

    public void testWrapWithMetricsSuccess() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), true);

        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics).recordSuccess(true);
        verify(metrics, never()).recordFailure(anyBoolean(), any());
        verify(metrics).recordTookTime(anyLong(), eq(true));
    }

    public void testWrapWithMetricsFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), true);

        Exception exception = new Exception("random failure");
        wrapped.onFailure(exception);

        verify(listener).onFailure(exception);
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics).recordFailure(true, exception);
        verify(metrics).recordTookTime(anyLong(), eq(true));
    }

    public void testWrapWithMetricsBulkFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), false);

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(
            List.of(new BulkItemResponse.Failure("0", "0", exception), new BulkItemResponse.Failure("1", "1", anotherException)),
            null
        );
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics).recordFailure(false, exception);
        verify(metrics).recordTookTime(anyLong(), eq(false));
    }

    public void testWrapWithMetricsSearchFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), true);

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(
            null,
            List.of(new ScrollableHitSource.SearchFailure(exception), new ScrollableHitSource.SearchFailure(anotherException))
        );
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics).recordFailure(true, exception);
        verify(metrics).recordTookTime(anyLong(), eq(true));
    }

    // listenerWithRelocations tests

    public void testListenerWithRelocationsPassesThroughForWorkerWithParent() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final Reindexer reindexer = reindexerWithRelocation();
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(new TaskId("node", 99), true);
        task.setWorker(Float.POSITIVE_INFINITY, null);

        final ActionListener<BulkByScrollResponse> original = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, original);

        assertSame(original, wrapped);
        verifyNoMoreInteractions(original);
    }

    public void testListenerWithRelocationsPassesThroughWhenNoRelocationRequested() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final Reindexer reindexer = reindexerWithRelocation();
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID, true);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        // do NOT call task.requestRelocation()

        final ActionListener<BulkByScrollResponse> original = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, original);

        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(original).onResponse(response);
        verify(original).delegateFailure(any());
        verifyNoMoreInteractions(original);
    }

    public void testListenerWithRelocationsPassesThroughWhenNoResumeInfo() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final Reindexer reindexer = reindexerWithRelocation();
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID, true);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        task.requestRelocation();

        final ActionListener<BulkByScrollResponse> original = spy(ActionListener.noop());
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, original);

        // response without ResumeInfo
        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(original).onResponse(response);
        verify(original).delegateFailure(any());
        verifyNoMoreInteractions(original);
    }

    public void testListenerWithRelocationsTriggersRelocationWhenResumeInfoPresent() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final Reindexer reindexer = reindexerWithRelocation();
        final BulkByScrollTask task = createTaskWithParentIdAndRelocationEnabled(TaskId.EMPTY_TASK_ID, true);
        task.setWorker(Float.POSITIVE_INFINITY, null);
        task.getWorkerState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        task.requestRelocation();

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        final ActionListener<BulkByScrollResponse> wrapped = reindexer.listenerWithRelocations(task, future);

        final BulkByScrollResponse response = reindexResponseWithResumeInfo();
        wrapped.onResponse(response);

        // currently fires onFailure with UnsupportedOperationException (TODO in production code)
        assertTrue(future.isDone());
        assertThat(expectThrows(Exception.class, future::actionGet), instanceOf(UnsupportedOperationException.class));
    }

    // --- workerListenerWithRelocationAndMetrics tests ---

    public void testWorkerListenerSkipsMetricsWhenRelocating() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final ReindexMetrics metrics = mock();
        final Reindexer reindexer = reindexerWithRelocationAndMetrics(metrics);
        final ActionListener<BulkByScrollResponse> outer = spy(ActionListener.noop());

        final var wrapped = reindexer.workerListenerWithRelocationAndMetrics(outer, randomNonNegativeLong(), randomBoolean());

        final BulkByScrollResponse response = reindexResponseWithResumeInfo();
        wrapped.onResponse(response);

        // metrics should NOT be recorded for a relocation response
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics, never()).recordFailure(anyBoolean(), any());
        verify(metrics, never()).recordTookTime(anyLong(), anyBoolean());
        // outer listener should still receive the response
        verify(outer).onResponse(response);

        verifyNoMoreInteractions(metrics, outer);
    }

    public void testWorkerListenerRecordsMetricsForNormalResponse() {
        assumeTrue("reindex resilience enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        final ReindexMetrics metrics = mock();
        final Reindexer reindexer = reindexerWithRelocationAndMetrics(metrics);
        final ActionListener<BulkByScrollResponse> outer = spy(ActionListener.noop());

        final var wrapped = reindexer.workerListenerWithRelocationAndMetrics(outer, randomNonNegativeLong(), true);

        final BulkByScrollResponse response = reindexResponseWithBulkAndSearchFailures(null, null);
        wrapped.onResponse(response);

        verify(outer).onResponse(response);
        verify(metrics).recordSuccess(true);
        verify(metrics).recordTookTime(anyLong(), eq(true));

        verifyNoMoreInteractions(metrics, outer);
    }

    // --- helpers ---

    private BulkByScrollResponse reindexResponseWithBulkAndSearchFailures(
        final List<BulkItemResponse.Failure> bulkFailures,
        final List<ScrollableHitSource.SearchFailure> searchFailures
    ) {
        return new BulkByScrollResponse(
            TimeValue.ZERO,
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            bulkFailures,
            searchFailures,
            false
        );
    }

    private BulkByScrollResponse reindexResponseWithResumeInfo() {
        final var workerResumeInfo = new ResumeInfo.ScrollWorkerResumeInfo(
            "test-scroll-id",
            System.nanoTime(),
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            null
        );
        return new BulkByScrollResponse(
            TimeValue.MINUS_ONE,
            new BulkByScrollTask.Status(List.of(), null),
            List.of(),
            List.of(),
            false,
            new ResumeInfo(workerResumeInfo, null)
        );
    }

    private static BulkByScrollTask createTaskWithParentIdAndRelocationEnabled(
        final TaskId parentTaskId,
        final boolean eligibleForRelocation
    ) {
        return new BulkByScrollTask(
            randomNonNegativeLong(),
            "test_type",
            "test_action",
            "test",
            parentTaskId,
            Collections.emptyMap(),
            eligibleForRelocation
        );
    }

    private static Reindexer reindexerWithRelocation() {
        return new Reindexer(
            mock(ClusterService.class),
            mock(ProjectResolver.class),
            mock(Client.class),
            mock(ThreadPool.class),
            mock(ScriptService.class),
            mock(ReindexSslConfig.class),
            null,
            mock(TaskManager.class),
            mock(ReindexRelocationNodePicker.class)
        );
    }

    private static Reindexer reindexerWithRelocationAndMetrics(final ReindexMetrics metrics) {
        return new Reindexer(
            mock(ClusterService.class),
            mock(ProjectResolver.class),
            mock(Client.class),
            mock(ThreadPool.class),
            mock(ScriptService.class),
            mock(ReindexSslConfig.class),
            metrics,
            mock(TaskManager.class),
            mock(ReindexRelocationNodePicker.class)
        );
    }
}
