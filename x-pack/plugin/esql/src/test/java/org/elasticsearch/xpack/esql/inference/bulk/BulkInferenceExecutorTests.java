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
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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

    public void testSuccessfulExecution() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(10_000);
        List<InferenceAction.Response> responses = randomInferenceResponseList(requests.size());

        InferenceRunner inferenceRunner = mockInferenceRunner(invocation -> {
            runWithRandomDelay(() -> {
                ActionListener<InferenceAction.Response> l = invocation.getArgument(1);
                l.onResponse(responses.get(requests.indexOf(invocation.getArgument(0, InferenceAction.Request.class))));
            });
            return null;
        });

        AtomicReference<List<InferenceAction.Response>> output = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(output::set, r -> fail("Unexpected exception"));

        bulkExecutor(inferenceRunner).execute(requestIterator(requests), listener);

        assertBusy(() -> assertThat(output.get(), allOf(notNullValue(), equalTo(responses))));
    }

    public void testSuccessfulExecutionOnEmptyRequest() throws Exception {
        BulkInferenceRequestIterator requestIterator = mock(BulkInferenceRequestIterator.class);
        when(requestIterator.hasNext()).thenReturn(false);

        AtomicReference<List<InferenceAction.Response>> output = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(output::set, r -> fail("Unexpected exception"));

        bulkExecutor(mock(InferenceRunner.class)).execute(requestIterator, listener);

        assertBusy(() -> assertThat(output.get(), allOf(notNullValue(), empty())));
    }

    public void testInferenceRunnerAlwaysFails() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(10_000);

        InferenceRunner inferenceRunner = mock(invocation -> {
            runWithRandomDelay(() -> {
                ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
                listener.onFailure(new RuntimeException("inference failure"));
            });
            return null;
        });

        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(r -> fail("Expceted exception"), exception::set);

        bulkExecutor(inferenceRunner).execute(requestIterator(requests), listener);

        assertBusy(() -> {
            assertThat(exception.get(), notNullValue());
            assertThat(exception.get().getMessage(), equalTo("inference failure"));
        });
    }

    public void testInferenceRunnerSometimesFails() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(10_000);

        InferenceRunner inferenceRunner = mockInferenceRunner(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(1);
            runWithRandomDelay(() -> {
                if ((requests.indexOf(invocation.getArgument(0, InferenceAction.Request.class)) % requests.size()) == 0) {
                    listener.onFailure(new RuntimeException("inference failure"));
                } else {
                    listener.onResponse(mockInferenceResponse());
                }
            });

            return null;
        });

        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(r -> fail("Expceted exception"), exception::set);

        bulkExecutor(inferenceRunner).execute(requestIterator(requests), listener);

        assertBusy(() -> {
            assertThat(exception.get(), notNullValue());
            assertThat(exception.get().getMessage(), equalTo("inference failure"));
        });
    }

    private BulkInferenceExecutor bulkExecutor(InferenceRunner inferenceRunner) {
        return new BulkInferenceExecutor(inferenceRunner, threadPool, randomBulkExecutionConfig());
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
        return new BulkInferenceExecutionConfig(between(1, 100), between(1, 100));
    }

    private BulkInferenceRequestIterator requestIterator(List<InferenceAction.Request> requests) {
        final Iterator<InferenceAction.Request> delegate = requests.iterator();
        BulkInferenceRequestIterator iterator = mock(BulkInferenceRequestIterator.class);
        doAnswer(i -> delegate.hasNext()).when(iterator).hasNext();
        doAnswer(i -> delegate.next()).when(iterator).next();
        doAnswer(i -> requests.size()).when(iterator).estimatedSize();
        return iterator;
    }

    private List<InferenceAction.Request> randomInferenceRequestList(int size) {
        List<InferenceAction.Request> requests = new ArrayList<>(size);
        while (requests.size() < size) {
            requests.add(this.mockInferenceRequest());
        }
        return requests;

    }

    private List<InferenceAction.Response> randomInferenceResponseList(int size) {
        List<InferenceAction.Response> response = new ArrayList<>(size);
        while (response.size() < size) {
            response.add(this.mockInferenceResponse());
        }
        return response;
    }

    private InferenceRunner mockInferenceRunner(Answer<Void> doInferenceAnswer) {
        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer(doInferenceAnswer).when(inferenceRunner).doInference(any(), any());
        return inferenceRunner;
    }

    private void runWithRandomDelay(Runnable runnable) {
        if (randomBoolean()) {
            runnable.run();
        } else {
            threadPool.schedule(
                runnable,
                TimeValue.timeValueNanos(between(1, 100)),
                threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME)
            );
        }
    }
}
