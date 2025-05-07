/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkInferenceExecutorTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setThreadPool() {
        threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
                1,
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulExecution() throws Exception {
        List<InferenceAction.Request> requests = Stream.generate(this::mockInferenceRequest).limit(between(1, 100)).toList();
        BulkInferenceRequestIterator requestIterator = requestIterator(requests);
        List<InferenceAction.Response> responses = Stream.generate(() -> mockInferenceResponse(RankedDocsResults.class))
            .limit(requests.size())
            .toList();

        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer((invocation) -> {
            ActionListener<InferenceAction.Response> l = invocation.getArgument(1);
            if (randomBoolean()) {
                Thread.sleep(between(0, 50));
            }
            l.onResponse(responses.get(requests.indexOf(invocation.getArgument(0, InferenceAction.Request.class))));
            return null;
        }).when(inferenceRunner).doInference(any(), any());

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);

        List<RankedDocsResults> output = new ArrayList<>();
        BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> outputBuilder = mock(BulkInferenceOutputBuilder.class);
        doAnswer(invocation -> {
            output.add(invocation.getArgument(0, RankedDocsResults.class));
            return null;
        }).when(outputBuilder).onInferenceResults(any());
        when(outputBuilder.buildOutput()).thenReturn(output);
        when(outputBuilder.inferenceResultsClass()).thenReturn(RankedDocsResults.class);

        BulkInferenceExecutor executor = bulkExecutor(inferenceRunner);
        executor.execute(requestIterator, outputBuilder, listener);

        assertBusy(() -> {
            assertThat(output, hasSize(requests.size()));
            assertThat(output, contains(responses.stream().map(InferenceAction.Response::getResults).toArray()));
            verify(listener).onResponse(eq(output));
        }, 60, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulExecutionOnEmptyRequest() throws Exception {
        BulkInferenceRequestIterator requestIterator = mock(BulkInferenceRequestIterator.class);
        when(requestIterator.hasNext()).thenReturn(false);

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);

        List<RankedDocsResults> output = new ArrayList<>();
        BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> outputBuilder = mock(BulkInferenceOutputBuilder.class);
        when(outputBuilder.buildOutput()).thenReturn(output);

        BulkInferenceExecutor executor = bulkExecutor(mock(InferenceRunner.class));
        executor.execute(requestIterator, outputBuilder, listener);

        assertBusy(() -> {
            assertThat(output, empty());
            verify(listener).onResponse(eq(output));
        });
    }

    @SuppressWarnings("unchecked")
    public void testInferenceRunnerAlwaysFails() throws Exception {
        List<InferenceAction.Request> requests = Stream.generate(this::mockInferenceRequest).limit(between(1, 10)).toList();
        BulkInferenceRequestIterator requestIterator = requestIterator(requests);

        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
            if (randomBoolean()) {
                Thread.sleep(between(0, 500));
            }
            listener.onFailure(new RuntimeException("inference failure"));
            return null;
        }).when(inferenceRunner).doInference(any(), any());

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        AtomicReference<Exception> e = new AtomicReference<>();
        doAnswer(i -> {
            e.set(i.getArgument(0, Exception.class));
            return null;
        }).when(listener).onFailure(any());

        BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> outputBuilder = mock(BulkInferenceOutputBuilder.class);

        BulkInferenceExecutor executor = bulkExecutor(inferenceRunner);
        executor.execute(requestIterator, outputBuilder, listener);

        assertBusy(() -> {
            verify(listener).onFailure(any(RuntimeException.class));
            assertThat(e.get().getMessage(), equalTo("inference failure"));
        });
    }

    @SuppressWarnings("unchecked")
    public void testInferenceRunnerSometimesFails() throws Exception {
        List<InferenceAction.Request> requests = Stream.generate(this::mockInferenceRequest).limit(between(2, 10)).toList();
        BulkInferenceRequestIterator requestIterator = requestIterator(requests);

        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
            if (randomBoolean()) {
                Thread.sleep(between(0, 500));
            }

            if (requests.indexOf(invocation.getArgument(0, InferenceAction.Request.class)) % requests.size() == 0) {
                listener.onFailure(new RuntimeException("inference failure"));
            } else {
                listener.onResponse(mockInferenceResponse(RankedDocsResults.class));
            }

            return null;
        }).when(inferenceRunner).doInference(any(), any());

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        AtomicReference<Exception> e = new AtomicReference<>();
        doAnswer(i -> {
            e.set(i.getArgument(0, Exception.class));
            return null;
        }).when(listener).onFailure(any());

        BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> outputBuilder = mock(BulkInferenceOutputBuilder.class);
        when(outputBuilder.inferenceResultsClass()).thenReturn(RankedDocsResults.class);

        BulkInferenceExecutor executor = bulkExecutor(inferenceRunner);
        executor.execute(requestIterator, outputBuilder, listener);

        assertBusy(() -> {
            verify(listener).onFailure(any(RuntimeException.class));
            assertThat(e.get().getMessage(), equalTo("inference failure"));
        });
    }

    @SuppressWarnings("unchecked")
    public void testBuildOutputFailure() throws Exception {
        List<InferenceAction.Request> requests = Stream.generate(this::mockInferenceRequest).limit(between(1, 10)).toList();
        BulkInferenceRequestIterator requestIterator = requestIterator(requests);

        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
            listener.onResponse(mockInferenceResponse(RankedDocsResults.class));
            return null;
        }).when(inferenceRunner).doInference(any(), any());

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        AtomicReference<Exception> e = new AtomicReference<>();
        doAnswer(i -> {
            e.set(i.getArgument(0, Exception.class));
            return null;
        }).when(listener).onFailure(any());

        BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> outputBuilder = mock(BulkInferenceOutputBuilder.class);
        when(outputBuilder.inferenceResultsClass()).thenReturn(RankedDocsResults.class);
        doThrow(new IllegalStateException("build output failure")).when(outputBuilder).buildOutput();

        BulkInferenceExecutor executor = bulkExecutor(inferenceRunner);

        executor.execute(requestIterator, outputBuilder, listener);

        assertBusy(() -> {
            verify(listener).onFailure(any(IllegalStateException.class));
            assertThat(e.get().getMessage(), equalTo("build output failure"));
        });
    }

    private BulkInferenceExecutor bulkExecutor(InferenceRunner inferenceRunner) {
        return new BulkInferenceExecutor(inferenceRunner, threadPool, randomBulkExecutionConfig());
    }

    private InferenceAction.Request mockInferenceRequest() {
        return mock(InferenceAction.Request.class);
    }

    private InferenceAction.Response mockInferenceResponse(Class<? extends InferenceServiceResults> resultClass) {
        InferenceAction.Response response = mock(InferenceAction.Response.class);
        when(response.getResults()).thenReturn(mock(resultClass));
        return response;
    }

    private BulkInferenceExecutionConfig randomBulkExecutionConfig() {
        return new BulkInferenceExecutionConfig(new TimeValue(between(1, 30), TimeUnit.SECONDS), between(1, 100));
    }

    private BulkInferenceRequestIterator requestIterator(List<InferenceAction.Request> requests) {
        final Iterator<InferenceAction.Request> delegate = requests.iterator();
        BulkInferenceRequestIterator iterator = mock(BulkInferenceRequestIterator.class);
        doAnswer(i -> delegate.hasNext()).when(iterator).hasNext();
        doAnswer(i -> delegate.next()).when(iterator).next();
        return iterator;
    }
}
