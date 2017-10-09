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

package org.elasticsearch.discovery.gce;

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.testing.util.MockSleeper;
import com.google.api.services.compute.Compute;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class RetryHttpInitializerWrapperTests extends ESTestCase {

    private static class FailThenSuccessBackoffTransport extends MockHttpTransport {

        public int lowLevelExecCalls;
        int errorStatusCode;
        int callsBeforeSuccess;
        boolean throwException;

        protected FailThenSuccessBackoffTransport(int errorStatusCode, int callsBeforeSuccess) {
            this.errorStatusCode = errorStatusCode;
            this.callsBeforeSuccess = callsBeforeSuccess;
            this.throwException = false;
        }

        protected FailThenSuccessBackoffTransport(int errorStatusCode, int callsBeforeSuccess, boolean throwException) {
            this.errorStatusCode = errorStatusCode;
            this.callsBeforeSuccess = callsBeforeSuccess;
            this.throwException = throwException;
        }

        public LowLevelHttpRequest retryableGetRequest = new MockLowLevelHttpRequest() {

            @Override
            public LowLevelHttpResponse execute() throws IOException {
                lowLevelExecCalls++;

                if (lowLevelExecCalls <= callsBeforeSuccess) {
                    if (throwException) {
                        throw new IOException("Test IOException");
                    }

                    // Return failure on the first call
                    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                    response.setContent("Request should fail");
                    response.setStatusCode(errorStatusCode);
                    return response;
                }
                // Return success on the second
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(200);
                return response;
            }
        };

        @Override
        public LowLevelHttpRequest buildRequest(String method, String url) {
            return retryableGetRequest;
        }
    }

    public void testSimpleRetry() throws Exception {
        FailThenSuccessBackoffTransport fakeTransport =
                new FailThenSuccessBackoffTransport(HttpStatusCodes.STATUS_CODE_SERVER_ERROR, 3);

        MockGoogleCredential credential = RetryHttpInitializerWrapper.newMockCredentialBuilder()
                .build();
        MockSleeper mockSleeper = new MockSleeper();

        RetryHttpInitializerWrapper retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential, mockSleeper,
            TimeValue.timeValueSeconds(5));

        Compute client = new Compute.Builder(fakeTransport, new JacksonFactory(), null)
                .setHttpRequestInitializer(retryHttpInitializerWrapper)
                .setApplicationName("test")
                .build();

        HttpRequest request = client.getRequestFactory().buildRequest("Get", new GenericUrl("http://elasticsearch.com"), null);
        HttpResponse response = request.execute();

        assertThat(mockSleeper.getCount(), equalTo(3));
        assertThat(response.getStatusCode(), equalTo(200));
    }

    public void testRetryWaitTooLong() throws Exception {
        TimeValue maxWaitTime = TimeValue.timeValueMillis(10);
        int maxRetryTimes = 50;

        FailThenSuccessBackoffTransport fakeTransport =
                new FailThenSuccessBackoffTransport(HttpStatusCodes.STATUS_CODE_SERVER_ERROR, maxRetryTimes);
        JsonFactory jsonFactory = new JacksonFactory();
        MockGoogleCredential credential = RetryHttpInitializerWrapper.newMockCredentialBuilder()
                .build();

        MockSleeper oneTimeSleeper = new MockSleeper() {
            @Override
            public void sleep(long millis) throws InterruptedException {
                Thread.sleep(maxWaitTime.getMillis());
                super.sleep(0); // important number, use this to get count
            }
        };

        RetryHttpInitializerWrapper retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential, oneTimeSleeper, maxWaitTime);

        Compute client = new Compute.Builder(fakeTransport, jsonFactory, null)
                .setHttpRequestInitializer(retryHttpInitializerWrapper)
                .setApplicationName("test")
                .build();

        HttpRequest request1 = client.getRequestFactory().buildRequest("Get", new GenericUrl("http://elasticsearch.com"), null);
        try {
            request1.execute();
            fail("Request should fail if wait too long");
        } catch (HttpResponseException e) {
            assertThat(e.getStatusCode(), equalTo(HttpStatusCodes.STATUS_CODE_SERVER_ERROR));
            // should only retry once.
            assertThat(oneTimeSleeper.getCount(), lessThan(maxRetryTimes));
        }
    }

    public void testIOExceptionRetry() throws Exception {
        FailThenSuccessBackoffTransport fakeTransport =
                new FailThenSuccessBackoffTransport(HttpStatusCodes.STATUS_CODE_SERVER_ERROR, 1, true);

        MockGoogleCredential credential = RetryHttpInitializerWrapper.newMockCredentialBuilder()
                .build();
        MockSleeper mockSleeper = new MockSleeper();
        RetryHttpInitializerWrapper retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential, mockSleeper,
            TimeValue.timeValueMillis(500));

        Compute client = new Compute.Builder(fakeTransport, new JacksonFactory(), null)
                .setHttpRequestInitializer(retryHttpInitializerWrapper)
                .setApplicationName("test")
                .build();

        HttpRequest request = client.getRequestFactory().buildRequest("Get", new GenericUrl("http://elasticsearch.com"), null);
        HttpResponse response = request.execute();

        assertThat(mockSleeper.getCount(), equalTo(1));
        assertThat(response.getStatusCode(), equalTo(200));
    }
}
