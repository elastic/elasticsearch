/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.*;

public class TransportActionFilterChainTests extends ElasticsearchTestCase {

    @Test
    public void testActionFilters() throws ExecutionException, InterruptedException {

        int numFilters = randomInt(10);
        Set<Integer> orders = new HashSet<>(numFilters);
        while (orders.size() < numFilters) {
            orders.add(randomInt(10));
        }

        Set<ActionFilter> filters = new HashSet<>();
        for (Integer order : orders) {
            filters.add(new TestFilter(order, randomFrom(Operation.values())));
        }

        String actionName = randomAsciiOfLength(randomInt(30));
        ActionFilters actionFilters = new ActionFilters(filters);
        TransportAction<TestRequest, TestResponse> transportAction = new TransportAction<TestRequest, TestResponse>(ImmutableSettings.EMPTY, actionName, null, actionFilters) {
            @Override
            protected void doExecute(TestRequest request, ActionListener<TestResponse> listener) {
                listener.onResponse(new TestResponse());
            }
        };

        ArrayList<ActionFilter> actionFiltersByOrder = Lists.newArrayList(filters);
        Collections.sort(actionFiltersByOrder, new Comparator<ActionFilter>() {
            @Override
            public int compare(ActionFilter o1, ActionFilter o2) {
                return Integer.compare(o1.order(), o2.order());
            }
        });

        List<ActionFilter> expectedActionFilters = Lists.newArrayList();
        boolean errorExpected = false;
        for (ActionFilter filter : actionFiltersByOrder) {
            TestFilter testFilter = (TestFilter) filter;
            expectedActionFilters.add(testFilter);
            if (testFilter.operation == Operation.LISTENER_FAILURE) {
                errorExpected = true;
            }
            if (!(testFilter.operation == Operation.CONTINUE_PROCESSING) ) {
                break;
            }
        }

        PlainListenableActionFuture<TestResponse> future = new PlainListenableActionFuture<>(false, null);
        transportAction.execute(new TestRequest(), future);
        try {
            assertThat(future.get(), notNullValue());
            assertThat("shouldn't get here if an error is expected", errorExpected, equalTo(false));
        } catch(Throwable t) {
            assertThat("shouldn't get here if an error is not expected " + t.getMessage(), errorExpected, equalTo(true));
        }

        List<TestFilter> testFiltersByLastExecution = Lists.newArrayList();
        for (ActionFilter actionFilter : actionFilters.filters()) {
            testFiltersByLastExecution.add((TestFilter) actionFilter);
        }
        Collections.sort(testFiltersByLastExecution, new Comparator<TestFilter>() {
            @Override
            public int compare(TestFilter o1, TestFilter o2) {
                return Long.compare(o1.lastExecution, o2.lastExecution);
            }
        });

        ArrayList<TestFilter> finalTestFilters = Lists.newArrayList();
        for (ActionFilter filter : testFiltersByLastExecution) {
            TestFilter testFilter = (TestFilter) filter;
            finalTestFilters.add(testFilter);
            if (!(testFilter.operation == Operation.CONTINUE_PROCESSING) ) {
                break;
            }
        }

        assertThat(finalTestFilters.size(), equalTo(expectedActionFilters.size()));
        for (int i = 0; i < finalTestFilters.size(); i++) {
            TestFilter testFilter = finalTestFilters.get(i);
            assertThat(testFilter, equalTo(expectedActionFilters.get(i)));
            assertThat(testFilter.runs.get(), equalTo(1));
            assertThat(testFilter.lastActionName, equalTo(actionName));
        }
    }

    @Test
    public void testTooManyContinueProcessing() throws ExecutionException, InterruptedException {

        final int additionalContinueCount = randomInt(10);

        TestFilter testFilter = new TestFilter(randomInt(), new Callback() {
            @Override
            public void execute(final String action, final ActionRequest actionRequest, final ActionListener actionListener, final ActionFilterChain actionFilterChain) {
                for (int i = 0; i <= additionalContinueCount; i++) {
                    new Thread() {
                        @Override
                        public void run() {
                            actionFilterChain.continueProcessing(action, actionRequest, actionListener);
                        }
                    }.start();
                }
            }
        });

        Set<ActionFilter> filters = new HashSet<>();
        filters.add(testFilter);

        String actionName = randomAsciiOfLength(randomInt(30));
        ActionFilters actionFilters = new ActionFilters(filters);
        TransportAction<TestRequest, TestResponse> transportAction = new TransportAction<TestRequest, TestResponse>(ImmutableSettings.EMPTY, actionName, null, actionFilters) {
            @Override
            protected void doExecute(TestRequest request, ActionListener<TestResponse> listener) {
                listener.onResponse(new TestResponse());
            }
        };

        final CountDownLatch latch = new CountDownLatch(additionalContinueCount + 1);
        final AtomicInteger responses = new AtomicInteger();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        transportAction.execute(new TestRequest(), new ActionListener<TestResponse>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                responses.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                failures.add(e);
                latch.countDown();
            }
        });

        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("timeout waiting for the filter to notify the listener as many times as expected");
        }

        assertThat(testFilter.runs.get(), equalTo(1));
        assertThat(testFilter.lastActionName, equalTo(actionName));

        assertThat(responses.get(), equalTo(1));
        assertThat(failures.size(), equalTo(additionalContinueCount));
        for (Throwable failure : failures) {
            assertThat(failure, instanceOf(IllegalStateException.class));
        }
    }

    private static class TestFilter implements ActionFilter {
        private final int order;
        private final Operation operation;
        private final Callback callback;

        AtomicInteger runs = new AtomicInteger();
        volatile String lastActionName;
        volatile long lastExecution = Long.MAX_VALUE; //the filters that don't run will go last in the sorted list

        TestFilter(int order, Operation operation) {
            this.order = order;
            this.operation = operation;
            this.callback = operation.callback;
        }

        TestFilter(int order, Callback callback) {
            this.order = order;
            this.operation = null; //custom operation
            this.callback = callback;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(String action, ActionRequest actionRequest, ActionListener actionListener, ActionFilterChain actionFilterChain) {
            this.runs.incrementAndGet();
            this.lastActionName = action;
            this.lastExecution = System.nanoTime();
            this.callback.execute(action, actionRequest, actionListener, actionFilterChain);
        }

        @Override
        public int order() {
            return order;
        }
    }

    private static enum Operation {

        CONTINUE_PROCESSING(new Callback() {
            @Override
            public void execute(String action, ActionRequest actionRequest, ActionListener actionListener, ActionFilterChain actionFilterChain) {
                actionFilterChain.continueProcessing(action, actionRequest, actionListener);
            }
        }),
        LISTENER_RESPONSE(new Callback() {
            @Override
            @SuppressWarnings("unchecked")
            public void execute(String action, ActionRequest actionRequest, ActionListener actionListener, ActionFilterChain actionFilterChain) {
                actionListener.onResponse(new TestResponse());
            }
        }),
        LISTENER_FAILURE(new Callback() {
            @Override
            public void execute(String action, ActionRequest actionRequest, ActionListener actionListener, ActionFilterChain actionFilterChain) {
                actionListener.onFailure(new ElasticsearchTimeoutException(""));
            }
        });

        private final Callback callback;

        Operation(Callback callback) {
            this.callback = callback;
        }
    }

    private static interface Callback {
        void execute(String action, ActionRequest actionRequest, ActionListener actionListener, ActionFilterChain actionFilterChain);
    }

    private static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {

    }
}
