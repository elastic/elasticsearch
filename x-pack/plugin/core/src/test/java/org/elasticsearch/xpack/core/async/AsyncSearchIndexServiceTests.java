/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
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

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.hamcrest.Matchers.equalTo;

// TODO: test CRUD operations
public class AsyncSearchIndexServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<TestAsyncResponse> indexService;

    public static class TestAsyncResponse implements AsyncResponse<TestAsyncResponse> {
        public final String test;
        public final long expirationTimeMillis;
        public String failure;

        public TestAsyncResponse(String test, long expirationTimeMillis) {
            this.test = test;
            this.expirationTimeMillis = expirationTimeMillis;
        }

        public TestAsyncResponse(String test, long expirationTimeMillis, String failure) {
            this.test = test;
            this.expirationTimeMillis = expirationTimeMillis;
            this.failure = failure;
        }

        public TestAsyncResponse(StreamInput input) throws IOException {
            test = input.readOptionalString();
            this.expirationTimeMillis = input.readLong();
            failure = input.readOptionalString();
        }

        @Override
        public long getExpirationTime() {
            return expirationTimeMillis;
        }

        @Override
        public TestAsyncResponse withExpirationTime(long expirationTime) {
            return new TestAsyncResponse(test, expirationTime, failure);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(test);
            out.writeLong(expirationTimeMillis);
            out.writeOptionalString(failure);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestAsyncResponse that = (TestAsyncResponse) o;
            return expirationTimeMillis == that.expirationTimeMillis
                && Objects.equals(test, that.test)
                && Objects.equals(failure, that.failure);
        }

        @Override
        public int hashCode() {
            return Objects.hash(test, expirationTimeMillis, failure);
        }

        @Override
        public String toString() {
            return "TestAsyncResponse{"
                + "test='"
                + test
                + '\''
                + "failure='"
                + failure
                + '\''
                + ", expirationTimeMillis="
                + expirationTimeMillis
                + '}';
        }

        @Override
        public TestAsyncResponse convertToFailure(Exception exc) {
            return new TestAsyncResponse(test, expirationTimeMillis, exc.getMessage());
        }
    }

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>(
            "test",
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            ASYNC_SEARCH_ORIGIN,
            TestAsyncResponse::new,
            writableRegistry(),
            bigArrays
        );
    }

    public void testEncodeSearchResponse() throws IOException {
        final int iterations = iterations(1, 20);
        for (int i = 0; i < iterations; i++) {
            long expirationTime = randomLong();
            String testMessage = randomAlphaOfLength(10);
            TestAsyncResponse initialResponse = new TestAsyncResponse(testMessage, expirationTime);
            AsyncExecutionId executionId = new AsyncExecutionId(
                Long.toString(randomNonNegativeLong()),
                new TaskId(randomAlphaOfLength(10), randomNonNegativeLong())
            );

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
                    throw new CircuitBreakingException(
                        "Current used [" + used + "] and requesting bytes [" + bytes + "] " + "is greater than the limit [" + limit + "]",
                        Durability.TRANSIENT
                    );
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

        void adjustLimit(long newLimit) {
            if (newLimit < used) {
                throw new IllegalArgumentException("Limit must not be smaller than used; used=" + used + "; limit=" + newLimit);
            }
            this.limit = newLimit;
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
        indexService = new AsyncTaskIndexService<>(
            "test",
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            ASYNC_SEARCH_ORIGIN,
            TestAsyncResponse::new,
            writableRegistry(),
            bigArrays
        );

        AsyncExecutionId executionId = new AsyncExecutionId(
            Long.toString(randomNonNegativeLong()),
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong())
        );
        long expirationTime = randomLong();
        String testMessage = randomAlphaOfLength(10);
        {
            circuitBreaker.adjustLimit(randomIntBetween(1, 64)); // small limit
            TestAsyncResponse initialResponse = new TestAsyncResponse(testMessage, expirationTime);
            PlainActionFuture<IndexResponse> createFuture = new PlainActionFuture<>();
            indexService.createResponse(executionId.getDocId(), Map.of(), initialResponse, createFuture);
            CircuitBreakingException e = expectThrows(CircuitBreakingException.class, createFuture::actionGet);
            assertEquals(0, e.getSuppressed().length); // no other suppressed exceptions
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
                CircuitBreakingException e = expectThrows(CircuitBreakingException.class, updateFuture::actionGet);
                assertEquals(0, e.getSuppressed().length); // no other suppressed exceptions
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

    public void testMaxAsyncSearchResponseSize() throws Exception {
        try {
            // successfully create an initial response
            AsyncExecutionId executionId1 = new AsyncExecutionId(
                Long.toString(randomNonNegativeLong()),
                new TaskId(randomAlphaOfLength(10), randomNonNegativeLong())
            );
            TestAsyncResponse initialResponse = new TestAsyncResponse(randomAlphaOfLength(130), randomLong());
            PlainActionFuture<IndexResponse> createFuture1 = new PlainActionFuture<>();
            indexService.createResponse(executionId1.getDocId(), Map.of(), initialResponse, createFuture1);
            createFuture1.actionGet();

            // setting very small limit for the max size of async search response
            int limit = randomIntBetween(1, 125);
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.transientSettings(Settings.builder().put("search.max_async_search_response_size", limit + "b"));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            String expectedErrMsg = "Can't store an async search response larger than ["
                + limit
                + "] bytes. "
                + "This limit can be set by changing the ["
                + MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()
                + "] setting.";

            // test that an update operation of the initial response fails
            PlainActionFuture<UpdateResponse> updateFuture = new PlainActionFuture<>();
            TestAsyncResponse updateResponse = new TestAsyncResponse(randomAlphaOfLength(130), randomLong());
            indexService.updateResponse(executionId1.getDocId(), Map.of(), updateResponse, updateFuture);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, updateFuture::actionGet);
            assertEquals(expectedErrMsg, e.getMessage());
            // test that the inital response is overwritten with a failure
            PlainActionFuture<TestAsyncResponse> getFuture = new PlainActionFuture<>();
            indexService.getResponse(executionId1, randomBoolean(), getFuture);
            assertEquals(expectedErrMsg, getFuture.actionGet().failure);

            // test that a create operation fails
            AsyncExecutionId executionId2 = new AsyncExecutionId(
                Long.toString(randomNonNegativeLong()),
                new TaskId(randomAlphaOfLength(10), randomNonNegativeLong())
            );
            PlainActionFuture<IndexResponse> createFuture = new PlainActionFuture<>();
            TestAsyncResponse initialResponse2 = new TestAsyncResponse(randomAlphaOfLength(130), randomLong());
            indexService.createResponse(executionId2.getDocId(), Map.of(), initialResponse2, createFuture);
            IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, createFuture::actionGet);
            assertEquals(expectedErrMsg, e2.getMessage());
        } finally {
            // restoring limit
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.transientSettings(Settings.builder().put("search.max_async_search_response_size", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

}
