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

package org.elasticsearch.client.advanced;

import org.junit.Assert;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class RequestTestUtil {
    /**
     * TODO: move that to a generic test util class
     * Check the validation of a request
     * @param request   Request to validate
     * @param exception Expected exception (null if we don't expect any)
     * @param message   Expected error message (can be a sub part of the full message)
     */
    public static void assertThrows(RestRequest request, Class<? extends Exception> exception, String message) {
        try {
            request.validate();
            Assert.fail("We were excepting an " + IllegalArgumentException.class.getName());
        } catch (Exception e) {
            Assert.assertThat(e, instanceOf(exception));
            Assert.assertThat(e.getMessage(), containsString(message));
        }
    }

    /**
     * TODO: move that to a generic test util class
     * Check the validation of a request
     * @param request   Request to validate
     */
    public static void assertNoException(RestRequest request) {
        request.validate();
    }

    /**
     * Utility Mock which helps checking asynchronous calls
     */
    public static class MockResponseListener<R extends RestResponse> implements RestResponseListener<R> {
        private final AtomicReference<R> response = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        @Override
        public void onSuccess(R response) {
            if (this.response.compareAndSet(null, response) == false) {
                throw new IllegalStateException("onSuccess was called multiple times");
            }
        }

        @Override
        public void onFailure(Exception exception) {
            if (this.exception.compareAndSet(null, exception) == false) {
                throw new IllegalStateException("onFailure was called multiple times");
            }
        }

        public Exception getException() {
            return exception.get();
        }

        public R getResponse() {
            return response.get();
        }
    }
}
