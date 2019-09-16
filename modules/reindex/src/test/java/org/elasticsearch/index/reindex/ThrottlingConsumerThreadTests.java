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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ThrottlingConsumerThreadTests extends ESTestCase {

    private ThreadPool threadPool;
    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testMultiThread() throws Exception {
        int throttleInterval = randomIntBetween(1, 20);
        CountDownLatch rounds = new CountDownLatch(randomIntBetween(10, 1000/throttleInterval)); // a second max
        // we only pass increasing numbers and keep track of the before and after value (latest and committed) in order to validate
        // the output. We use that the throttler must read the value again after invoking the consumer to enforce that the next
        // consumer invocation must see at least the committed values from last round.
        ConcurrentMap<Thread, Long> latest = new ConcurrentHashMap<>();
        ConcurrentMap<Thread, Long> committed = new ConcurrentHashMap<>();
        ConcurrentMap<Thread, Long> lastRoundCommitted = new ConcurrentHashMap<>();
        BiConsumer<Tuple<Thread, Long>, Runnable> validatingConsumer = new BiConsumer<>() {
            private AtomicBoolean active = new AtomicBoolean();
            @Override
            public void accept(Tuple<Thread, Long> value, Runnable whenDone) {
                assertTrue(active.compareAndSet(false, true));
                assertThat(value.v2(), greaterThanOrEqualTo(lastRoundCommitted.get(value.v1())));
                assertThat(value.v2(), lessThanOrEqualTo(latest.get(value.v1())));
                lastRoundCommitted.putAll(committed);
                Runnable work = () -> {
                    try {
                        Thread.sleep(1); // simulate hard work
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    assertTrue(active.compareAndSet(true, false));
                    whenDone.run();
                    rounds.countDown();
                };
                if (randomBoolean()) {
                    work.run();
                } else {
                    threadPool.generic().submit(work);
                }
            }
        };
        ThrottlingConsumer<Tuple<Thread, Long>> throttler
            = new ThrottlingConsumer<>(validatingConsumer, TimeValue.timeValueMillis(throttleInterval), System::nanoTime, threadPool);
        AtomicBoolean stopped = new AtomicBoolean();
        List<Thread> threads = IntStream.range(0, randomIntBetween(2, 5)).mapToObj(i -> new Thread(getTestName() + "-" + i) {
            private long value = randomLongBetween(0, Integer.MAX_VALUE);
            {
                setDaemon(true);
            }

            public void run() {
                while (!stopped.get()) {
                    latest.put(this, value);
                    throttler.accept(Tuple.tuple(this, value));
                    committed.put(this, value);
                    value = value + randomIntBetween(0, Integer.MAX_VALUE);
                }
            }
        }).collect(Collectors.toList());

        threads.forEach(t -> {
            committed.put(t, Long.MIN_VALUE);
            lastRoundCommitted.put(t, Long.MIN_VALUE);
            latest.put(t, Long.MIN_VALUE);
        });

        CountDownLatch closed = new CountDownLatch(1);
        AutoCloseable shutdown = () -> {
            throttler.close(() -> closed.countDown());
            stopped.set(true);

            threads.forEach(t -> {
                try {
                    t.join(10000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });

            threads.forEach(t -> assertFalse(t.isAlive()));
        };

        threads.forEach(Thread::start);

        try (shutdown) {
            assertTrue(rounds.await(10, TimeUnit.SECONDS));
        }
    }
}
