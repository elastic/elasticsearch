/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ConsistentCacheTests extends ESTestCase {

    private ConsistentCache<String, String> consistentCache;

    @Before
    public void setup() {
        consistentCache = new ConsistentCache<>(CacheBuilder.<String, String>builder().build());
    }

    public void testItemWillCached() {
        final ConsistentCache.Checkpoint<String, String> checkpoint = consistentCache.checkpoint();
        checkpoint.put("foo", "bar");
        assertEquals("bar", consistentCache.get("foo"));
    }

    public void testItemWillNotBeCachedWhenInvalidationHappensBetweenCheckpointAndItsUsage() throws InterruptedException {
        final ConsistentCache.Checkpoint<String, String> checkpoint = consistentCache.checkpoint();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            consistentCache.invalidate(List.of("fizz"));
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();
        checkpoint.put("foo", "bar");
        assertNull(consistentCache.get("foo"));
    }

    public void testInvalidate() {
        final ConsistentCache.Checkpoint<String, String> checkpoint = consistentCache.checkpoint();
        checkpoint.put("foo", "bar");
        checkpoint.put("fizz", "buzz");
        checkpoint.put("hello", "world");

        assertEquals(3, consistentCache.count());
        assertEquals("bar", consistentCache.get("foo"));
        assertEquals("buzz", consistentCache.get("fizz"));
        assertEquals("world", consistentCache.get("hello"));

        consistentCache.invalidate(List.of("foo", "hello"));
        assertEquals(1, consistentCache.count());
        assertEquals("buzz", consistentCache.get("fizz"));
        assertNull(consistentCache.get("foo"));
        assertNull(consistentCache.get("hello"));
    }

    public void testInvalidateAll() {
        final ConsistentCache.Checkpoint<String, String> checkpoint = consistentCache.checkpoint();
        checkpoint.put("foo", "bar");
        checkpoint.put("fizz", "buzz");
        checkpoint.put("hello", "world");

        consistentCache.invalidateAll();
        assertEquals(0, consistentCache.count());
    }
}
