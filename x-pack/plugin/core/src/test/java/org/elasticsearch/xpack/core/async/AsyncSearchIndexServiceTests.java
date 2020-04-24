/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.hamcrest.Matchers.equalTo;

// TODO: test CRUD operations
public class AsyncSearchIndexServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<TestAsyncResponse> indexService;

    public static class TestAsyncResponse implements AsyncResponse<TestAsyncResponse> {
        private final String test;
        private final long expirationTimeMillis;

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
            return 0;
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
    }

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>("test", clusterService, transportService.getThreadPool().getThreadContext(),
            client(), ASYNC_SEARCH_ORIGIN, TestAsyncResponse::new, writableRegistry());
    }

    public void testEncodeSearchResponse() throws IOException {
        for (int i = 0; i < 10; i++) {
            TestAsyncResponse response = new TestAsyncResponse(randomAlphaOfLength(10), randomLong());
            String encoded = indexService.encodeResponse(response);
            TestAsyncResponse same = indexService.decodeResponse(encoded);
            assertThat(same, equalTo(response));
        }
    }
}
