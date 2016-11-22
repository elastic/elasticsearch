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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RequestTestUtil {
    /**
     * Utility Mock which helps checking asynchronous calls
     */
    public static class MockConsumer<R> implements Consumer<R> {
        private final AtomicReference<R> response = new AtomicReference<>();
        @Override
        public void accept(R response) {
            if (this.response.compareAndSet(null, response) == false) {
                throw new IllegalStateException("onSuccess was called multiple times");
            }
        }

        public R get() {
            return response.get();
        }
    }
}
