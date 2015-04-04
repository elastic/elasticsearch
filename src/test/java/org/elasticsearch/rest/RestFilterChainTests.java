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

package org.elasticsearch.rest;

import com.google.common.collect.Lists;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;

public class RestFilterChainTests extends ElasticsearchTestCase {

    @Test
    public void testRestFilters() throws InterruptedException {

        RestController restController = new RestController(ImmutableSettings.EMPTY);

        int numFilters = randomInt(10);
        Set<Integer> orders = new HashSet<>(numFilters);
        while (orders.size() < numFilters) {
            orders.add(randomInt(10));
        }

        List<RestFilter> filters = new ArrayList<>();
        for (Integer order : orders) {
            TestFilter testFilter = new TestFilter(order, randomFrom(Operation.values()));
            filters.add(testFilter);
            restController.registerFilter(testFilter);
        }

        ArrayList<RestFilter> restFiltersByOrder = Lists.newArrayList(filters);
        Collections.sort(restFiltersByOrder, new Comparator<RestFilter>() {
            @Override
            public int compare(RestFilter o1, RestFilter o2) {
                return Integer.compare(o1.order(), o2.order());
            }
        });

        List<RestFilter> expectedRestFilters = Lists.newArrayList();
        for (RestFilter filter : restFiltersByOrder) {
            TestFilter testFilter = (TestFilter) filter;
            expectedRestFilters.add(testFilter);
            if (!(testFilter.callback == Operation.CONTINUE_PROCESSING) ) {
                break;
            }
        }

        restController.registerHandler(RestRequest.Method.GET, "/", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
                channel.sendResponse(new TestResponse());
            }
        });

        FakeRestRequest fakeRestRequest = new FakeRestRequest();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, 1);
        restController.dispatchRequest(fakeRestRequest, fakeRestChannel);
        assertThat(fakeRestChannel.await(), equalTo(true));


        List<TestFilter> testFiltersByLastExecution = Lists.newArrayList();
        for (RestFilter restFilter : filters) {
            testFiltersByLastExecution.add((TestFilter)restFilter);
        }
        Collections.sort(testFiltersByLastExecution, new Comparator<TestFilter>() {
            @Override
            public int compare(TestFilter o1, TestFilter o2) {
                return Long.compare(o1.executionToken, o2.executionToken);
            }
        });

        ArrayList<TestFilter> finalTestFilters = Lists.newArrayList();
        for (RestFilter filter : testFiltersByLastExecution) {
            TestFilter testFilter = (TestFilter) filter;
            finalTestFilters.add(testFilter);
            if (!(testFilter.callback == Operation.CONTINUE_PROCESSING) ) {
                break;
            }
        }

        assertThat(finalTestFilters.size(), equalTo(expectedRestFilters.size()));

        for (int i = 0; i < finalTestFilters.size(); i++) {
            TestFilter testFilter = finalTestFilters.get(i);
            assertThat(testFilter, equalTo(expectedRestFilters.get(i)));
            assertThat(testFilter.runs.get(), equalTo(1));
        }
    }

    @Test
    public void testTooManyContinueProcessing() throws InterruptedException {

        final int additionalContinueCount = randomInt(10);

        TestFilter testFilter = new TestFilter(randomInt(), new Callback() {
            @Override
            public void execute(final RestRequest request, final RestChannel channel, final RestFilterChain filterChain) throws Exception {
                for (int i = 0; i <= additionalContinueCount; i++) {
                    filterChain.continueProcessing(request, channel);
                }
            }
        });

        RestController restController = new RestController(ImmutableSettings.EMPTY);
        restController.registerFilter(testFilter);

        restController.registerHandler(RestRequest.Method.GET, "/", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
                channel.sendResponse(new TestResponse());
            }
        });

        FakeRestRequest fakeRestRequest = new FakeRestRequest();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, additionalContinueCount + 1);
        restController.dispatchRequest(fakeRestRequest, fakeRestChannel);
        fakeRestChannel.await();

        assertThat(testFilter.runs.get(), equalTo(1));

        assertThat(fakeRestChannel.responses.get(), equalTo(1));
        assertThat(fakeRestChannel.errors.get(), equalTo(additionalContinueCount));
    }

    private static class FakeRestChannel extends RestChannel {

        private final CountDownLatch latch;
        AtomicInteger responses = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        protected FakeRestChannel(RestRequest request, int responseCount) {
            super(request, randomBoolean());
            this.latch = new CountDownLatch(responseCount);
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return super.newBuilder();
        }

        @Override
        public XContentBuilder newBuilder(@Nullable BytesReference autoDetectSource) throws IOException {
            return super.newBuilder(autoDetectSource);
        }

        @Override
        protected BytesStreamOutput newBytesOutput() {
            return super.newBytesOutput();
        }

        @Override
        public RestRequest request() {
            return super.request();
        }

        @Override
        public void sendResponse(RestResponse response) {
            if (response.status() == RestStatus.OK) {
                responses.incrementAndGet();
            } else {
                errors.incrementAndGet();
            }
            latch.countDown();
        }

        public boolean await() throws InterruptedException {
            return latch.await(10, TimeUnit.SECONDS);
        }
    }

    private static enum Operation implements Callback {
        CONTINUE_PROCESSING {
            @Override
            public void execute(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
                filterChain.continueProcessing(request, channel);
            }
        },
        CHANNEL_RESPONSE {
            @Override
            public void execute(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
                channel.sendResponse(new TestResponse());
            }
        }
    }

    private static interface Callback {
        void execute(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception;
    }

    private final AtomicInteger counter = new AtomicInteger();

    private class TestFilter extends RestFilter {
        private final int order;
        private final Callback callback;
        AtomicInteger runs = new AtomicInteger();
        volatile int executionToken = Integer.MAX_VALUE; //the filters that don't run will go last in the sorted list

        TestFilter(int order, Callback callback) {
            this.order = order;
            this.callback = callback;
        }

        @Override
        public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
            this.runs.incrementAndGet();
            this.executionToken = counter.incrementAndGet();
            this.callback.execute(request, channel, filterChain);
        }

        @Override
        public int order() {
            return order;
        }

        @Override
        public String toString() {
            return "[order:" + order + ", executionToken:" + executionToken + "]";
        }
    }

    private static class TestResponse extends RestResponse {
        @Override
        public String contentType() {
            return null;
        }

        @Override
        public BytesReference content() {
            return null;
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }
    }
}
