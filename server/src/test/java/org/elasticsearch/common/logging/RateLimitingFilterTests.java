/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    /**
     * Check that messages are rate-limited by their key.
     */
    public void testMessagesAreRateLimitedByKey() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key " + i, "", "msg " + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "", "msg " + 0);
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 129", "", "msg " + 129);
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key0 was evicted from the cache
        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "", "msg " + 0);
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that messages are rate-limited by their x-opaque-id value
     */
    public void testMessagesAreRateLimitedByXOpaqueId() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "", "id " + i, "msg " + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "", "id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "", "id 129", "msg 129");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key0 was evicted from the cache
        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "", "id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that messages are rate-limited by their key and x-opaque-id value
     */
    public void testMessagesAreRateLimitedByKeyAndXOpaqueId() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key " + i, "opaque-id " + i, "msg " + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "opaque-id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 129", "opaque-id 129", "msg 129");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key 0 was evicted from the cache
        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "opaque-id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that it is the combination of key and x-opaque-id that rate-limits messages, by varying each
     * independently and checking that a message is not filtered.
     */
    public void testVariationsInKeyAndXOpaqueId() {
        Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "opaque-id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "opaque-id 0", "msg 0");
        // Rejected because the "x-opaque-id" and "key" values are the same as above
        assertThat(filter.filter(message), equalTo(Result.DENY));

        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 1", "opaque-id 0", "msg 0");
        // Accepted because the "key" value is different
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key 0", "opaque-id 1", "msg 0");
        // Accepted because the "x-opaque-id" value is different
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that rate-limiting is not applied to messages if they are not an EsLogMessage.
     */
    public void testOnlyEsMessagesAreFiltered() {
        Message message = new SimpleMessage("a message");
        assertThat(filter.filter(message), equalTo(Result.NEUTRAL));
    }

    /**
     * Check that the filter can be reset, so that previously-seen keys are treated as new keys.
     */
    public void testFilterCanBeReset() {
        final Message message = DeprecatedMessage.of(DeprecationCategory.OTHER, "key", "", "msg");

        // First time, the message is a allowed
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Second time, it is filtered out
        assertThat(filter.filter(message), equalTo(Result.DENY));

        filter.reset();

        // Third time, it is allowed again
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }
}
