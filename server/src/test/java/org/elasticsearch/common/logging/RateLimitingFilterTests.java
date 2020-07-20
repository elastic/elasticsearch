/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.apache.logging.log4j.core.Filter.Result;
import static org.hamcrest.Matchers.equalTo;

public class RateLimitingFilterTests extends ESTestCase {

    private RateLimitingFilter filter;

    @Before
    public void setup() {
        this.filter = new RateLimitingFilter();
        filter.start();
    }

    @After
    public void cleanup() {
        this.filter.stop();
    }

    public void testRateLimitingFilter() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = DeprecatedMessage.of("", "msg " + i).field("key", "key" + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = DeprecatedMessage.of("", "msg " + 0).field("key", "key" + 0);
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = DeprecatedMessage.of("", "msg " + 129).field("key", "key" + 129);
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key0 was evicted from the cache
        message = DeprecatedMessage.of("", "msg " + 0).field("key", "key" + 0);
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    public void testOnlyEsMessagesAreFiltered() {
        Message message = new SimpleMessage("a message");
        assertThat(filter.filter(message), equalTo(Result.NEUTRAL));
    }
}
