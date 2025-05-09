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
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
                between(1, 20),
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
        List<InferenceAction.Request> requests = randomInferenceRequestList(between(1, 50));
        List<InferenceAction.Response> responses = randomInferenceResponseList(requests.size());

        InferenceRunner inferenceRunner = mockInferenceRunner(invocation -> {
            ActionListener<InferenceAction.Response> l = invocation.getArgument(1);
            l.onResponse(responses.get(requests.indexOf(invocation.getArgument(0, InferenceAction.Request.class))));
            return null;
        });

        AtomicReference<List<InferenceAction.Response>> output = new AtomicReference<>();
        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            output.set(invocation.getArgument(0, List.class));
            return null;
        }).when(listener).onResponse(any());

        bulkExecutor(inferenceRunner).execute(requestIterator(requests), mockOutputBuilder(), listener);

        assertBusy(() -> {
            verify(listener).onResponse(any());
            verify(listener, never()).onFailure(any());
            assertThat(
                output.get(),
                allOf(
                    notNullValue(),
                    hasSize(requests.size()),
                    contains(responses.stream().map(InferenceAction.Response::getResults).toArray())
                )
            );
        });
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulExecutionOnEmptyRequest() throws Exception {
        BulkInferenceRequestIterator requestIterator = mock(BulkInferenceRequestIterator.class);
        when(requestIterator.hasNext()).thenReturn(false);

        AtomicReference<List<RankedDocsResults>> output = new AtomicReference<>();
        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            output.set(invocation.getArgument(0, List.class));
            return null;
        }).when(listener).onResponse(any());

        bulkExecutor(mock(InferenceRunner.class)).execute(requestIterator, mockOutputBuilder(), listener);

        assertBusy(() -> {
            verify(listener).onResponse(any());
            verify(listener, never()).onFailure(any());
            assertThat(output.get(), allOf(notNullValue(), empty()));
        });
    }

    @SuppressWarnings("unchecked")
    public void testInferenceRunnerAlwaysFails() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(between(1, 30));

        InferenceRunner inferenceRunner = mock(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("inference failure"));
            return null;
        });

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        AtomicReference<Exception> e = new AtomicReference<>();
        doAnswer(i -> {
            e.set(i.getArgument(0, Exception.class));
            return null;
        }).when(listener).onFailure(any());

        bulkExecutor(inferenceRunner).execute(requestIterator(requests), mockOutputBuilder(), listener);

        assertBusy(() -> {
            verify(listener).onFailure(any(RuntimeException.class));
            verify(listener, never()).onResponse(any());
            assertThat(e.get().getMessage(), equalTo("inference failure"));
        });
    }

    @SuppressWarnings("unchecked")
    public void testInferenceRunnerSometimesFails() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(between(2, 30));

        InferenceRunner inferenceRunner = mockInferenceRunner(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
            if ((requests.indexOf(invocation.getArgument(0, InferenceAction.Request.class)) % requests.size()) == 0) {
                listener.onFailure(new RuntimeException("inference failure"));
            } else {
                listener.onResponse(mockInferenceResponse());
            }

            return null;
        });

        ActionListener<List<RankedDocsResults>> listener = mock(ActionListener.class);
        AtomicReference<Exception> e = new AtomicReference<>();
        doAnswer(i -> {
            e.set(i.getArgument(0, Exception.class));
            return null;
        }).when(listener).onFailure(any());

        bulkExecutor(inferenceRunner).execute(requestIterator(requests), mockOutputBuilder(), listener);

        assertBusy(() -> {
            verify(listener).onFailure(any(RuntimeException.class));
            verify(listener, never()).onResponse(any());
            assertThat(e.get().getMessage(), equalTo("inference failure"));
        });
    }

    private BulkInferenceExecutor<RankedDocsResults, List<RankedDocsResults>> bulkExecutor(InferenceRunner inferenceRunner) {
        return new BulkInferenceExecutor<>(inferenceRunner, threadPool, randomBulkExecutionConfig());
    }

    private InferenceAction.Request mockInferenceRequest() {
        return mock(InferenceAction.Request.class);
    }

    private InferenceAction.Response mockInferenceResponse() {
        InferenceAction.Response response = mock(InferenceAction.Response.class);
        when(response.getResults()).thenReturn(mock(RankedDocsResults.class));
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

    private List<InferenceAction.Request> randomInferenceRequestList(int size) {
        return Stream.generate(this::mockInferenceRequest).limit(size).toList();
    }

    private List<InferenceAction.Response> randomInferenceResponseList(int size) {
        return Stream.generate(this::mockInferenceResponse).limit(size).toList();
    }

    private InferenceRunner mockInferenceRunner(Answer<Void> doInferenceAnswer) {
        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer(doInferenceAnswer).when(inferenceRunner).doInference(any(), any());
        return inferenceRunner;
    }

    @SuppressWarnings("unchecked")
    private BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> mockOutputBuilder() {
        List<RankedDocsResults> output = new ArrayList<>();
        BulkInferenceOutputBuilder<RankedDocsResults, List<RankedDocsResults>> outputBuilder = mock(BulkInferenceOutputBuilder.class);
        when(outputBuilder.buildOutput()).thenReturn(output);
        when(outputBuilder.inferenceResultsClass()).thenReturn(RankedDocsResults.class);
        doAnswer(invocationOnMock -> {
            output.add(invocationOnMock.getArgument(0, RankedDocsResults.class));
            return null;
        }).when(outputBuilder).addInferenceResults(any());
        return outputBuilder;
    }
}
