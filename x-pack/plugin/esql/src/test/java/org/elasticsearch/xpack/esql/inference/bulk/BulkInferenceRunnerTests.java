/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkInferenceRunnerTests extends ESTestCase {
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

    public void testSuccessfulBulkExecution() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(between(1, 1_000));
        List<InferenceAction.Response> responses = randomInferenceResponseList(requests.size());

        Client client = mockClient(invocation -> {
            runWithRandomDelay(() -> {
                ActionListener<InferenceAction.Response> l = invocation.getArgument(2);
                l.onResponse(responses.get(requests.indexOf(invocation.getArgument(1, InferenceAction.Request.class))));
            });
            return null;
        });

        AtomicReference<List<InferenceAction.Response>> output = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(output::set, ESTestCase::fail);

        inferenceRunnerFactory(client).create(randomBulkExecutionConfig()).executeBulk(requestIterator(requests), listener);

        assertBusy(() -> assertThat(output.get(), allOf(notNullValue(), equalTo(responses))));
    }

    public void testSuccessfulBulkExecutionOnEmptyRequest() throws Exception {
        BulkInferenceRequestIterator requestIterator = mock(BulkInferenceRequestIterator.class);
        when(requestIterator.hasNext()).thenReturn(false);

        AtomicReference<List<InferenceAction.Response>> output = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(output::set, ESTestCase::fail);

        inferenceRunnerFactory(new NoOpClient(threadPool)).create(randomBulkExecutionConfig()).executeBulk(requestIterator, listener);

        assertBusy(() -> assertThat(output.get(), allOf(notNullValue(), empty())));
    }

    public void testBulkExecutionWhenInferenceRunnerAlwaysFails() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(between(1, 1000));

        Client client = mockClient(invocation -> {
            runWithRandomDelay(() -> {
                ActionListener<InferenceAction.Response> listener = invocation.getArgument(2);
                listener.onFailure(new RuntimeException("inference failure"));
            });
            return null;
        });

        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(r -> fail("Expected an exception"), exception::set);

        inferenceRunnerFactory(client).create(randomBulkExecutionConfig()).executeBulk(requestIterator(requests), listener);

        assertBusy(() -> {
            assertThat(exception.get(), notNullValue());
            assertThat(exception.get().getMessage(), equalTo("inference failure"));
        });
    }

    public void testBulkExecutionWhenInferenceRunnerSometimesFails() throws Exception {
        List<InferenceAction.Request> requests = randomInferenceRequestList(between(1, 1_000));

        Client client = mockClient(invocation -> {
            ActionListener<InferenceAction.Response> listener = invocation.getArgument(2);
            runWithRandomDelay(() -> {
                if ((requests.indexOf(invocation.getArgument(1, InferenceAction.Request.class)) % requests.size()) == 0) {
                    listener.onFailure(new RuntimeException("inference failure"));
                } else {
                    listener.onResponse(mockInferenceResponse());
                }
            });

            return null;
        });

        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(r -> fail("Expected an exception"), exception::set);

        inferenceRunnerFactory(client).create(randomBulkExecutionConfig()).executeBulk(requestIterator(requests), listener);

        assertBusy(() -> {
            assertThat(exception.get(), notNullValue());
            assertThat(exception.get().getMessage(), equalTo("inference failure"));
        });
    }

    public void testParallelBulkExecution() throws Exception {
        int batches = between(50, 100);
        CountDownLatch latch = new CountDownLatch(batches);

        for (int i = 0; i < batches; i++) {
            runWithRandomDelay(() -> {
                List<InferenceAction.Request> requests = randomInferenceRequestList(between(1, 1_000));
                List<InferenceAction.Response> responses = randomInferenceResponseList(requests.size());

                Client client = mockClient(invocation -> {
                    runWithRandomDelay(() -> {
                        ActionListener<InferenceAction.Response> l = invocation.getArgument(2);
                        l.onResponse(responses.get(requests.indexOf(invocation.getArgument(1, InferenceAction.Request.class))));
                    });
                    return null;
                });

                ActionListener<List<InferenceAction.Response>> listener = ActionListener.wrap(r -> {
                    assertThat(r, equalTo(responses));
                    latch.countDown();
                }, ESTestCase::fail);

                inferenceRunnerFactory(client).create(randomBulkExecutionConfig()).executeBulk(requestIterator(requests), listener);
            });
        }

        latch.await(10, TimeUnit.SECONDS);
    }

    private BulkInferenceRunner.Factory inferenceRunnerFactory(Client client) {
        return BulkInferenceRunner.factory(client);
    }

    private InferenceAction.Request mockInferenceRequest() {
        return mock(InferenceAction.Request.class);
    }

    private InferenceAction.Response mockInferenceResponse() {
        InferenceAction.Response response = mock(InferenceAction.Response.class);
        when(response.getResults()).thenReturn(mock(RankedDocsResults.class));
        return response;
    }

    private BulkInferenceRunnerConfig randomBulkExecutionConfig() {
        return new BulkInferenceRunnerConfig(between(1, 100), between(1, 100));
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
            requests.add(mockInferenceRequest());
        }
        return requests;

    }

    private List<InferenceAction.Response> randomInferenceResponseList(int size) {
        List<InferenceAction.Response> response = new ArrayList<>(size);
        while (response.size() < size) {
            response.add(mockInferenceResponse());
        }
        return response;
    }

    private Client mockClient(Answer<Void> doInferenceAnswer) {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(doInferenceAnswer).when(client).execute(eq(InferenceAction.INSTANCE), any(InferenceAction.Request.class), any());
        return client;
    }

    private void runWithRandomDelay(Runnable runnable) {
        if (randomBoolean()) {
            runnable.run();
        } else {
            threadPool.schedule(runnable, TimeValue.timeValueNanos(between(1, 100_000)), threadPool.generic());
        }
    }
}
