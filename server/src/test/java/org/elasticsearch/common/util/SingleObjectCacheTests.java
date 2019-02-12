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

package org.elasticsearch.common.util;

import org.elasticsearch.common.unit.TimeValue;
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
