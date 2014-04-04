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

package org.elasticsearch.benchmark.common.recycler;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.recycler.AbstractRecyclerC;
import org.elasticsearch.common.recycler.Recycler;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.recycler.Recyclers.*;

/** Benchmark that tries to measure the overhead of object recycling depending on concurrent access. */
public class RecyclerBenchmark {

    private static final long NUM_RECYCLES = 5000000L;
    private static final Random RANDOM = new Random(0);

    private static long bench(final Recycler<?> recycler, long numRecycles, int numThreads) throws InterruptedException {
        final AtomicLong recycles = new AtomicLong(numRecycles);
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; ++i){
            // Thread ids happen to be generated sequentially, so we also generate random threads so that distribution of IDs
            // is not perfect for the concurrent recycler
            for (int j = RANDOM.nextInt(5); j >= 0; --j) {
                new Thread();
            }

            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    while (recycles.getAndDecrement() > 0) {
                        final Recycler.V<?> v = recycler.obtain();
                        v.close();
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
        }
        final long start = System.nanoTime();
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        return System.nanoTime() - start;
    }

    public static void main(String[] args) throws InterruptedException {
        final int limit = 100;
        final Recycler.C<Object> c = new AbstractRecyclerC<Object>() {

            @Override
            public Object newInstance(int sizing) {
                return new Object();
            }

            @Override
            public void recycle(Object value) {
                // do nothing
            }
        };

        final ImmutableMap<String, Recycler<Object>> recyclers = ImmutableMap.<String, Recycler<Object>>builder()
                .put("none", none(c))
                .put("concurrent-queue", concurrentDeque(c, limit))
                .put("locked", locked(deque(c, limit)))
                .put("concurrent", concurrent(dequeFactory(c, limit), Runtime.getRuntime().availableProcessors()))
                .put("soft-concurrent", concurrent(softFactory(dequeFactory(c, limit)), Runtime.getRuntime().availableProcessors())).build();

        // warmup
        final long start = System.nanoTime();
        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10)) {
            for (Recycler<?> recycler : recyclers.values()) {
                bench(recycler, NUM_RECYCLES, 2);
            }
        }

        // run
        for (int numThreads = 1; numThreads <= 4 * Runtime.getRuntime().availableProcessors(); numThreads *= 2) {
            System.out.println("## " + numThreads + " threads\n");
            System.gc();
            Thread.sleep(1000);
            for (Recycler<?> recycler : recyclers.values()) {
                bench(recycler, NUM_RECYCLES, numThreads);
            }
            for (int i = 0; i < 5; ++i) {
                for (Map.Entry<String, Recycler<Object>> entry : recyclers.entrySet()) {
                    System.out.println(entry.getKey() + "\t" + TimeUnit.NANOSECONDS.toMillis(bench(entry.getValue(), NUM_RECYCLES, numThreads)));
                }
                System.out.println();
            }
        }
    }

}
