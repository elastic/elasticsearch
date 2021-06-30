/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.hamcrest.Matchers.equalTo;

// TODO: test CRUD operations
public class AsyncSearchIndexServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<TestAsyncResponse> indexService;

    public static class TestAsyncResponse implements AsyncResponse<TestAsyncResponse> {
        public final String test;
        public final long expirationTimeMillis;

        public TestAsyncResponse(String test, long expirationTimeMillis) {
            this.test = test;
            this.expirationTimeMillis = expirationTimeMillis;
        }

        public TestAsyncResponse(StreamInput input) throws IOException {
            test = input.readOptionalString();
            this.expirationTimeMillis = input.readLong();
        }

        @Override
        public long getExpirationTime() {
            return expirationTimeMillis;
        }

        @Override
        public TestAsyncResponse withExpirationTime(long expirationTime) {
            return new TestAsyncResponse(test, expirationTime);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(test);
            out.writeLong(expirationTimeMillis);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestAsyncResponse that = (TestAsyncResponse) o;
            return expirationTimeMillis == that.expirationTimeMillis &&
                Objects.equals(test, that.test);
        }

        @Override
        public int hashCode() {
            return Objects.hash(test, expirationTimeMillis);
        }

        @Override
        public String toString() {
            return "TestAsyncResponse{" +
                "test='" + test + '\'' +
                ", expirationTimeMillis=" + expirationTimeMillis +
                '}';
        }
    }

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>("test", clusterService, transportService.getThreadPool().getThreadContext(),
            client(), ASYNC_SEARCH_ORIGIN, TestAsyncResponse::new, writableRegistry(), bigArrays);
    }

    public void testEncodeSearchResponse() throws IOException {
        final int iterations = iterations(1, 20);
        for (int i = 0; i < iterations; i++) {
            long expirationTime = randomLong();
            String testMessage = randomAlphaOfLength(10);
            TestAsyncResponse initialResponse = new TestAsyncResponse(testMessage, expirationTime);
            AsyncExecutionId executionId = new AsyncExecutionId(
                Long.toString(randomNonNegativeLong()),
                new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()));

            PlainActionFuture<IndexResponse> createFuture = new PlainActionFuture<>();
            indexService.createResponse(executionId.getDocId(), Map.of(), initialResponse, createFuture);
            assertThat(createFuture.actionGet().getResult(), equalTo(DocWriteResponse.Result.CREATED));

            if (randomBoolean()) {
                PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
                indexService.getResponse(executionId, randomBoolean(), getFuture);
                assertThat(getFuture.actionGet(), equalTo(initialResponse));
            }

            int updates = randomIntBetween(1, 5);
            for (int u = 0; u < updates; u++) {
                if (randomBoolean()) {
                    testMessage = randomAlphaOfLength(10);
                    TestAsyncResponse updateResponse = new TestAsyncResponse(testMessage, randomLong());
                    PlainActionFuture<UpdateResponse> updateFuture = new PlainActionFuture<>();
                    indexService.updateResponse(executionId.getDocId(), Map.of(), updateResponse, updateFuture);
                    updateFuture.actionGet();
                } else {
                    expirationTime = randomLong();
                    PlainActionFuture<UpdateResponse> updateFuture = new PlainActionFuture<>();
                    indexService.updateExpirationTime(executionId.getDocId(), expirationTime, updateFuture);
                    updateFuture.actionGet();
                }
                if (randomBoolean()) {
                    PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
                    indexService.getResponse(executionId, randomBoolean(), getFuture);
                    assertThat(getFuture.actionGet().test, equalTo(testMessage));
                    assertThat(getFuture.actionGet().expirationTimeMillis, equalTo(expirationTime));
                }
            }
        }
    }

    static class AdjustableLimitCircuitBreaker extends NoopCircuitBreaker {
        private long used = 0;
        private long limit = 0;

        AdjustableLimitCircuitBreaker(String name) {
            super(name);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (bytes <= 0) {
                addWithoutBreaking(bytes);
            } else {
                if (used + bytes > limit) {
                    throw new CircuitBreakingException("Current used [" + used + "] and requesting bytes [" + bytes + "] " +
                        "is greater than the limit [" + limit + "]", Durability.TRANSIENT);
                }
                used += bytes;
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }

        @Override
        public long getLimit() {
            return limit;
        }

        void adjustLimit(long limit) {
            if (limit < used) {
                throw new IllegalArgumentException("Limit must not be smaller than used; used=" + used + "; limit=" + limit);
            }
            this.limit = limit;
        }
    }

    public void testCircuitBreaker() throws Exception {
        AdjustableLimitCircuitBreaker circuitBreaker = new AdjustableLimitCircuitBreaker("test");
        CircuitBreakerService circuitBreakerService = new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                assertThat(name, equalTo(CircuitBreaker.REQUEST));
                return circuitBreaker;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                return null;
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                return null;
            }
        };
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, CircuitBreaker.REQUEST);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>("test", clusterService, transportService.getThreadPool().getThreadContext(),
            client(), ASYNC_SEARCH_ORIGIN, TestAsyncResponse::new, writableRegistry(), bigArrays);

        AsyncExecutionId executionId = new AsyncExecutionId(
            Long.toString(randomNonNegativeLong()),
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()));
        long expirationTime = randomLong();
        String testMessage = randomAlphaOfLength(10);
        {
            circuitBreaker.adjustLimit(randomIntBetween(1, 64)); // small limit
            TestAsyncResponse initialResponse = new TestAsyncResponse(testMessage, expirationTime);
            PlainActionFuture<IndexResponse> createFuture = new PlainActionFuture<>();
            indexService.createResponse(executionId.getDocId(), Map.of(), initialResponse, createFuture);
            expectThrows(CircuitBreakingException.class, createFuture::actionGet);
            assertThat(circuitBreaker.getUsed(), equalTo(0L));
        }
        {
            circuitBreaker.adjustLimit(randomIntBetween(16 * 1024, 1024 * 1024)); // large enough
            TestAsyncResponse initialResponse = new TestAsyncResponse(testMessage, expirationTime);
            PlainActionFuture<IndexResponse> createFuture = new PlainActionFuture<>();
            indexService.createResponse(executionId.getDocId(), Map.of(), initialResponse, createFuture);
            assertThat(createFuture.actionGet().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(circuitBreaker.getUsed(), equalTo(0L));
            if (randomBoolean()) {
                PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
                indexService.getResponse(executionId, randomBoolean(), getFuture);
                assertThat(getFuture.actionGet(), equalTo(initialResponse));
                assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));
            }
            if (randomBoolean()) {
                circuitBreaker.adjustLimit(between(1, 16));
                PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
                indexService.getResponse(executionId, randomBoolean(), getFuture);
                expectThrows(CircuitBreakingException.class, getFuture::actionGet);
                assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));
            }
        }

        int updates = randomIntBetween(1, 5);
        for (int u = 0; u < updates; u++) {
            if (randomBoolean()) {
                circuitBreaker.adjustLimit(randomIntBetween(16 * 1024, 1024 * 1024));
                testMessage = randomAlphaOfLength(10);
                TestAsyncResponse updateResponse = new TestAsyncResponse(testMessage, randomLong());
                PlainActionFuture<UpdateResponse> updateFuture = new PlainActionFuture<>();
                indexService.updateResponse(executionId.getDocId(), Map.of(), updateResponse, updateFuture);
                updateFuture.actionGet();
                assertThat(circuitBreaker.getUsed(), equalTo(0L));
            } else {
                circuitBreaker.adjustLimit(randomIntBetween(1, 64)); // small limit
                PlainActionFuture<UpdateResponse> updateFuture = new PlainActionFuture<>();
                TestAsyncResponse updateResponse = new TestAsyncResponse(randomAlphaOfLength(100), randomLong());
                indexService.updateResponse(executionId.getDocId(), Map.of(), updateResponse, updateFuture);
                expectThrows(CircuitBreakingException.class, updateFuture::actionGet);
                assertThat(circuitBreaker.getUsed(), equalTo(0L));
            }
            if (randomBoolean()) {
                circuitBreaker.adjustLimit(randomIntBetween(16 * 1024, 1024 * 1024)); // small limit
                PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
                indexService.getResponse(executionId, randomBoolean(), getFuture);
                assertThat(getFuture.actionGet().test, equalTo(testMessage));
                assertThat(getFuture.actionGet().expirationTimeMillis, equalTo(expirationTime));
                assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));
            }
            if (randomBoolean()) {
                circuitBreaker.adjustLimit(randomIntBetween(1, 16)); // small limit
                PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
                indexService.getResponse(executionId, randomBoolean(), getFuture);
                expectThrows(CircuitBreakingException.class, getFuture::actionGet);
                assertBusy(() -> assertThat(circuitBreaker.getUsed(), equalTo(0L)));
            }
        }
    }
}
