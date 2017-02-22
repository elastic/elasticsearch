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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RestClientTests extends RestClientTestCase {

    public void testPerformAsyncWithUnsupportedMethod() throws Exception {
        TestListener listener = new TestListener();
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("unsupported", randomAsciiOfLength(5), listener);
            listener.waitForCompletion();
        }

        Exception exception = listener.getException();
        assertNotNull(exception);
        assertTrue(exception instanceof UnsupportedOperationException);
        assertEquals("http method not supported: unsupported", exception.getMessage());
    }

    public void testPerformAsyncWithNullParams() throws Exception {
        TestListener listener = new TestListener();
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync(randomAsciiOfLength(5), randomAsciiOfLength(5), null, listener);
            listener.waitForCompletion();
        }

        Exception exception = listener.getException();
        assertNotNull(exception);
        assertTrue(exception instanceof NullPointerException);
        assertEquals("params must not be null", exception.getMessage());
    }

    public void testPerformAsyncWithNullHeaders() throws Exception {
        TestListener listener = new TestListener();
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("GET", randomAsciiOfLength(5), listener, null);
            listener.waitForCompletion();
        }

        Exception exception = listener.getException();
        assertNotNull(exception);
        assertTrue(exception instanceof NullPointerException);
        assertEquals("request headers must not be null", exception.getMessage());
    }

    public void testPerformAsyncWithWrongEndpoint() throws Exception {
        TestListener listener = new TestListener();
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("GET", "::http:///", listener);
            listener.waitForCompletion();
        }

        Exception exception = listener.getException();
        assertNotNull(exception);
        assertTrue(exception instanceof IllegalArgumentException);
        assertEquals("Expected scheme name at index 0: ::http:///", exception.getMessage());
    }

    private static RestClient createRestClient() {
        return new RestClient(mock(CloseableHttpAsyncClient.class), randomLongBetween(1_000, 30_000), new Header[]{},
                new HttpHost[]{new HttpHost("localhost", 9200)}, null, null);
    }

    class TestListener implements ResponseListener {

        private final CountDownLatch countDown = new CountDownLatch(1);
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        @Override
        public void onSuccess(Response theResponse) {
            if (response.getAndSet(theResponse) == null) {
                countDown.countDown();
            }
        }

        @Override
        public void onFailure(Exception theException) {
            if (exception.getAndSet(theException) == null) {
                countDown.countDown();
            }
        }

        void waitForCompletion() throws InterruptedException {
            countDown.await();
        }

        public Response getResponse() {
            return response.get();
        }

        Exception getException() {
            return exception.get();
        }
    }
}
