/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.settings.SecureReleasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.rest.SecureReleasableRestChannel.collectSecureReleasableTransients;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SecureReleasableRestChannelTests extends ESTestCase {

    public void testCollectWithNestedMap() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        AtomicBoolean directlyReleased = new AtomicBoolean();
        threadContext.putTransient("direct", (SecureReleasable) () -> directlyReleased.set(true));

        AtomicBoolean groupedReleased = new AtomicBoolean();
        threadContext.putTransient("grouped", Map.of("secondary_token", (SecureReleasable) () -> groupedReleased.set(true)));

        // a Releasable that is not also a SecureReleasable is not
        AtomicBoolean plainReleased = new AtomicBoolean();
        threadContext.putTransient("plain", (Releasable) () -> plainReleased.set(true));

        threadContext.putTransient("unrelated_map", Map.of("k1", "v1", "k2", 42));
        threadContext.putTransient("unrelated_value", "just a header value");

        Releasable collected = collectSecureReleasableTransients(threadContext);
        assertThat(collected, notNullValue());
        collected.close();

        assertTrue("a direct SecureReleasable transient must be released", directlyReleased.get());
        assertTrue("a SecureReleasable nested inside a Map transient must be released", groupedReleased.get());
        assertFalse("a Releasable that isn't also a SecureReleasable must not be released", plainReleased.get());
    }

    public void testCollectReturnsNullWhenNothingMatches() {
        assertThat(collectSecureReleasableTransients(new ThreadContext(Settings.EMPTY)), nullValue());

        ThreadContext noMatches = new ThreadContext(Settings.EMPTY);
        noMatches.putTransient("plain", (Releasable) () -> {});
        noMatches.putTransient("unrelated_map", Map.of("k1", "v1", "k2", 42));
        assertThat(collectSecureReleasableTransients(noMatches), nullValue());
    }

    public void testReleaseOnClose() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AtomicBoolean released = new AtomicBoolean();
        threadContext.putTransient("token", (SecureReleasable) () -> released.set(true));

        RestChannel delegate = mock(RestChannel.class);
        SecureReleasableRestChannel channel = new SecureReleasableRestChannel(delegate, threadContext);
        RestResponse response = new RestResponse(RestStatus.OK, "test");

        channel.sendResponse(response);

        verify(delegate).sendResponse(response);
        assertFalse(released.get());

        response.close();

        assertTrue(released.get());
    }
}
