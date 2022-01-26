/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleObjectCacheTests extends ESTestCase {

    public void testRefresh() {
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicBoolean needsRefresh = new AtomicBoolean(true);
        SingleObjectCache<Integer> cache = new SingleObjectCache<Integer>(TimeValue.timeValueMillis(100000), 0) {

            @Override
            protected Integer refresh() {
                return count.incrementAndGet();
            }

            @Override
            protected boolean needsRefresh() {
                return needsRefresh.get();
            }
        };
        assertEquals(1, cache.getOrRefresh().intValue());
        assertEquals(2, cache.getOrRefresh().intValue());
        needsRefresh.set(false);
        assertEquals(2, cache.getOrRefresh().intValue());
        needsRefresh.set(true);
        assertEquals(3, cache.getOrRefresh().intValue());
    }

    public void testRefreshDoesntBlock() throws InterruptedException {
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicBoolean needsRefresh = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch waiting = new CountDownLatch(1);
        final SingleObjectCache<Integer> cache = new SingleObjectCache<Integer>(TimeValue.timeValueMillis(1000), 0) {

            @Override
            protected Integer refresh() {
                if (count.get() == 1) {
                    try {
                        waiting.countDown();
                        latch.await();
                    } catch (InterruptedException e) {
                        assert false;
                    }
                }
                return count.incrementAndGet();
            }

            @Override
            protected boolean needsRefresh() {
                return needsRefresh.get();
            }
        };
        assertEquals(1, cache.getOrRefresh().intValue());
        needsRefresh.set(true);
        Thread t = new Thread() {
            @Override
            public void run() {
                Integer value = cache.getOrRefresh();
                assertEquals(2, value.intValue());
            }
        };
        t.start();
        waiting.await();
        assertEquals(1, cache.getOrRefresh().intValue());
        needsRefresh.set(false);
        latch.countDown();
        t.join();
        assertEquals(2, cache.getOrRefresh().intValue());

    }
}
