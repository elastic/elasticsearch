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

package org.elasticsearch.common.recycler;

import org.elasticsearch.common.lease.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LockedRecyclerTests extends AbstractRecyclerTestCase {

    @Override
    protected Recycler<byte[]> newRecycler(int limit) {
        return Recyclers.locked(Recyclers.deque(RECYCLER_C, limit));
    }

    public void testConcurrentCloseAndObtain() throws Exception {
        final int prepopulatedAmount = 1_000_000;
        final Recycler<byte[]> recycler = newRecycler(prepopulatedAmount);
        final List<Recycler.V<byte[]>> recyclers = new ArrayList<>();
        // prepopulate recycler with 1 million entries to ensure we have entries to iterate over
        for (int i = 0; i < prepopulatedAmount; i++) {
            recyclers.add(recycler.obtain());
        }
        Releasables.close(recyclers);
        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween((numberOfProcessors + 1) / 2, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(100_000, 1_000_000);
        final CountDownLatch latch = new CountDownLatch(1 + numberOfThreads);
        List<Thread> threads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads - 1; i++) {
            threads.add(new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (int iter = 0; iter < numberOfIterations; iter++) {
                        Recycler.V<byte[]> o = recycler.obtain();
                        final byte[] bytes = o.v();
                        assertNotNull(bytes);
                        o.close();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        threads.add(new Thread(() -> {
            latch.countDown();
            try {
                latch.await();
                Thread.sleep(randomLongBetween(1, 200));
                recycler.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        threads.forEach(Thread::start);
        latch.countDown();
        for (Thread t : threads) {
            t.join();
        }
    }
}
