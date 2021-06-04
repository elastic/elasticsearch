/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

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
            Map<String, Object> fields = new HashMap<>();
            int numFields = randomIntBetween(0, 100);
            for (int f = 0; f < numFields; f++) {
                fields.put("field-" + f, randomFrom(randomInt(), randomAlphaOfLength(100)));
            }
            BytesReference bytes = BytesReference.bytes(indexService.buildSourceForUpdateRequest(response, fields));
            Map<String, Object> maps = XContentHelper.convertToMap(bytes, randomBoolean()).v2();
            assertThat(maps, aMapWithSize(numFields + 1));
            for (Map.Entry<String, Object> e : fields.entrySet()) {
                assertThat(maps, hasEntry(e.getKey(), e.getValue()));
            }
            assertThat(maps, hasKey(AsyncTaskIndexService.RESULT_FIELD));
            final String encodedValue = (String) maps.get(AsyncTaskIndexService.RESULT_FIELD);
            TestAsyncResponse same = indexService.decodeResponse(encodedValue);
            assertThat(same, equalTo(response));
        }
    }
}
