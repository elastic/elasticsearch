/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class TransportActionFilterChainTests extends ESTestCase {

    private AtomicInteger counter;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        counter = new AtomicInteger();
        threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TransportActionFilterChainTests").build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders()
        );
    }

    @After
    public void shutdown() throws Exception {
        terminate(threadPool);
    }

    public void testActionFiltersRequest() throws InterruptedException {
        int numFilters = randomInt(10);
        Set<Integer> orders = Sets.newHashSetWithExpectedSize(numFilters);
        while (orders.size() < numFilters) {
            orders.add(randomInt(10));
        }

        Set<ActionFilter> filters = new HashSet<>();
        for (Integer order : orders) {
            filters.add(new RequestTestFilter(order, randomFrom(RequestOperation.values())));
        }

        String actionName = randomAlphaOfLength(randomInt(30));
        ActionFilters actionFilters = new ActionFilters(filters);
        TransportAction<TestRequest, TestResponse> transportAction = new TransportAction<TestRequest, TestResponse>(
            actionName,
            actionFilters,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
                listener.onResponse(new TestResponse());
            }
        };

        ArrayList<ActionFilter> actionFiltersByOrder = new ArrayList<>(filters);
        actionFiltersByOrder.sort(Comparator.comparingInt(ActionFilter::order));

        List<ActionFilter> expectedActionFilters = new ArrayList<>();
        boolean errorExpected = false;
        for (ActionFilter filter : actionFiltersByOrder) {
            RequestTestFilter testFilter = (RequestTestFilter) filter;
            expectedActionFilters.add(testFilter);
            if (testFilter.callback == RequestOperation.LISTENER_FAILURE) {
                errorExpected = true;
            }
            if (testFilter.callback != RequestOperation.CONTINUE_PROCESSING) {
                break;
            }
        }

        PlainActionFuture<TestResponse> future = new PlainActionFuture<>();

        ActionTestUtils.execute(transportAction, null, new TestRequest(), future);
        try {
            assertThat(future.get(), notNullValue());
            assertThat("shouldn't get here if an error is expected", errorExpected, equalTo(false));
        } catch (ExecutionException e) {
            assertThat("shouldn't get here if an error is not expected " + e.getMessage(), errorExpected, equalTo(true));
        }

        List<RequestTestFilter> testFiltersByLastExecution = new ArrayList<>();
        for (ActionFilter actionFilter : actionFilters.filters()) {
            testFiltersByLastExecution.add((RequestTestFilter) actionFilter);
        }

        testFiltersByLastExecution.sort(Comparator.comparingInt(o -> o.executionToken));

        ArrayList<RequestTestFilter> finalTestFilters = new ArrayList<>();
        for (ActionFilter filter : testFiltersByLastExecution) {
            RequestTestFilter testFilter = (RequestTestFilter) filter;
            finalTestFilters.add(testFilter);
            if (testFilter.callback != RequestOperation.CONTINUE_PROCESSING) {
                break;
            }
        }

        assertThat(finalTestFilters.size(), equalTo(expectedActionFilters.size()));
        for (int i = 0; i < finalTestFilters.size(); i++) {
            RequestTestFilter testFilter = finalTestFilters.get(i);
            assertThat(testFilter, equalTo(expectedActionFilters.get(i)));
            assertThat(testFilter.runs.get(), equalTo(1));
            assertThat(testFilter.lastActionName, equalTo(actionName));
        }
    }

    public void testTooManyContinueProcessingRequest() throws InterruptedException {
        RequestTestFilter testFilter = new RequestTestFilter(randomInt(), new RequestCallback() {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void execute(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> actionFilterChain
            ) {
                // expected proceed() call:
                actionFilterChain.proceed(task, action, request, listener);

                // extra, invalid, proceed() call:
                actionFilterChain.proceed(task, action, request, listener);
            }
        });

        Set<ActionFilter> filters = new HashSet<>();
        filters.add(testFilter);

        final CountDownLatch latch = new CountDownLatch(2);
        String actionName = randomAlphaOfLength(randomInt(30));
        ActionFilters actionFilters = new ActionFilters(filters);
        TransportAction<TestRequest, TestResponse> transportAction = new TransportAction<TestRequest, TestResponse>(
            actionName,
            actionFilters,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
                latch.countDown();
            }
        };

        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ActionTestUtils.execute(transportAction, null, new TestRequest(), new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                fail("should not complete listener");
            }

            @Override
            public void onFailure(Exception e) {
                failures.add(e);
            }
        }, latch));

        if (latch.await(10, TimeUnit.SECONDS) == false) {
            fail("timeout waiting for the filter to notify the listener as many times as expected");
        }

        assertThat(testFilter.runs.get(), equalTo(1));
        assertThat(testFilter.lastActionName, equalTo(actionName));

        assertThat(failures.size(), equalTo(1));
        for (Throwable failure : failures) {
            assertThat(failure, instanceOf(IllegalStateException.class));
        }
    }

    private class RequestTestFilter implements ActionFilter {
        private final RequestCallback callback;
        private final int order;
        AtomicInteger runs = new AtomicInteger();
        volatile String lastActionName;
        volatile int executionToken = Integer.MAX_VALUE; // the filters that don't run will go last in the sorted list

        RequestTestFilter(int order, RequestCallback callback) {
            this.order = order;
            this.callback = callback;
        }

        @Override
        public int order() {
            return order;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain
        ) {
            this.runs.incrementAndGet();
            this.lastActionName = action;
            this.executionToken = counter.incrementAndGet();
            this.callback.execute(task, action, request, listener, chain);
        }
    }

    private enum RequestOperation implements RequestCallback {
        CONTINUE_PROCESSING {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void execute(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> actionFilterChain
            ) {
                actionFilterChain.proceed(task, action, request, listener);
            }
        },
        LISTENER_RESPONSE {
            @Override
            @SuppressWarnings("unchecked")  // Safe because its all we test with
            public <Request extends ActionRequest, Response extends ActionResponse> void execute(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> actionFilterChain
            ) {
                ((ActionListener<TestResponse>) listener).onResponse(new TestResponse());
            }
        },
        LISTENER_FAILURE {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void execute(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> actionFilterChain
            ) {
                listener.onFailure(new ElasticsearchTimeoutException(""));
            }
        }
    }

    private interface RequestCallback {
        <Request extends ActionRequest, Response extends ActionResponse> void execute(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> actionFilterChain
        );
    }

    public static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
