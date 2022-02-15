/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class InvalidationCountingCacheWrapperTests extends ESTestCase {

    private InvalidationCountingCacheWrapper<String, String> invalidationCountingCacheWrapper;

    @Before
    public void setup() {
        invalidationCountingCacheWrapper = new InvalidationCountingCacheWrapper<>(CacheBuilder.<String, String>builder().build());
    }

    public void testItemWillCached() {
        final long invalidationCount = invalidationCountingCacheWrapper.getInvalidationCount();
        assertTrue(invalidationCountingCacheWrapper.putIfNoInvalidationSince("foo", "bar", invalidationCount));
        assertEquals("bar", invalidationCountingCacheWrapper.get("foo"));
    }

    public void testItemWillNotBeCachedIfInvalidationCounterHasChanged() throws InterruptedException {
        final long invalidationCount = invalidationCountingCacheWrapper.getInvalidationCount();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            invalidationCountingCacheWrapper.invalidate(List.of("fizz"));
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("foo", "bar", invalidationCount);
        assertNull(invalidationCountingCacheWrapper.get("foo"));
    }

    public void testInvalidate() {
        final long invalidationCount = invalidationCountingCacheWrapper.getInvalidationCount();
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("foo", "bar", invalidationCount);
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("fizz", "buzz", invalidationCount);
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("hello", "world", invalidationCount);

        assertEquals(3, invalidationCountingCacheWrapper.count());
        assertEquals("bar", invalidationCountingCacheWrapper.get("foo"));
        assertEquals("buzz", invalidationCountingCacheWrapper.get("fizz"));
        assertEquals("world", invalidationCountingCacheWrapper.get("hello"));

        invalidationCountingCacheWrapper.invalidate(List.of("foo", "hello"));
        assertEquals(1, invalidationCountingCacheWrapper.count());
        assertEquals("buzz", invalidationCountingCacheWrapper.get("fizz"));
        assertNull(invalidationCountingCacheWrapper.get("foo"));
        assertNull(invalidationCountingCacheWrapper.get("hello"));
    }

    public void testInvalidateAll() {
        final long invalidationCount = invalidationCountingCacheWrapper.getInvalidationCount();
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("foo", "bar", invalidationCount);
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("fizz", "buzz", invalidationCount);
        invalidationCountingCacheWrapper.putIfNoInvalidationSince("hello", "world", invalidationCount);

        invalidationCountingCacheWrapper.invalidateAll();
        assertEquals(0, invalidationCountingCacheWrapper.count());
    }
}
