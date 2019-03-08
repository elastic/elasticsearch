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
package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportClusterStateActionTests extends ESTestCase {

    public void testClusterStateSizeByVersionCacheDoesNotDuplicateWork() throws Exception {

        final int cacheSize = TransportClusterStateAction.ClusterStateSizeByVersionCache.CACHE_SIZE;
        final List<AtomicInteger> callCounts
            = IntStream.range(0, cacheSize).mapToObj(i -> new AtomicInteger()).collect(Collectors.toList());

        final int threadCount = cacheSize * 3;

        final List<PlainActionFuture<Integer>> resultFutures
            = IntStream.range(0, threadCount).mapToObj(i -> new PlainActionFuture<Integer>()).collect(Collectors.toList());

        final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount + 1);
        final long seed = randomLong();

        final String testExceptionMessage = "test IOException";

        final ThreadPool threadPool = new TestThreadPool("test");
        try {
            final TransportClusterStateAction.ClusterStateSizeByVersionCache cache
                = new TransportClusterStateAction.ClusterStateSizeByVersionCache(threadPool);

            final int expiredVersion = cacheSize + 1;
            {
                // add an entry to the cache so we can check it was pushed out of the cache later
                final int expiredSize = randomIntBetween(0, 100);
                cache.getOrComputeCachedSize(expiredVersion, () -> expiredSize,
                    ActionListener.wrap(i -> assertThat(i, equalTo(expiredSize)), Assert::assertNotNull));
            }

            final List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(() -> {
                try {
                    final Random random = new Random(seed + i);
                    final int clusterStateVersion = i % cacheSize;
                    final int size = random.nextInt();
                    cyclicBarrier.await();
                    cache.getOrComputeCachedSize(clusterStateVersion, () -> {
                        callCounts.get(clusterStateVersion).incrementAndGet();
                        if (rarely(random)) {
                            throw new IOException(testExceptionMessage);
                        } else {
                            return size;
                        }
                    }, resultFutures.get(i));

                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new AssertionError(e);
                }
            })).collect(Collectors.toList());

            threads.forEach(Thread::start);
            cyclicBarrier.await();
            for (Thread thread : threads) {
                thread.join();
            }

            for (PlainActionFuture<Integer> resultFuture : resultFutures) {
                try {
                    resultFuture.actionGet();
                } catch (Exception e) {
                    final Throwable rootCause = new ElasticsearchException(e).getRootCause();
                    assertThat(e.toString(), rootCause, instanceOf(IOException.class));
                    assertThat(e.toString(), rootCause.getMessage(), equalTo(testExceptionMessage));
                }
            }

            for (AtomicInteger callCount : callCounts) {
                assertThat(callCount.get(), equalTo(1));
            }

            {
                // check the expired entry was pushed out of the cache so requires recalculation
                final int newExpiredSize = randomIntBetween(0, 100);
                final PlainActionFuture<Integer> future = new PlainActionFuture<>();
                cache.getOrComputeCachedSize(expiredVersion, () -> newExpiredSize, future);
                assertThat(future.actionGet(), equalTo(newExpiredSize));
            }

        } finally {
            threadPool.shutdown();
        }
    }
}
