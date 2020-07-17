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

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

public class ListenableFutureTests extends ESTestCase {

    private ExecutorService executorService;
    private ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    @After
    public void stopExecutorService() throws InterruptedException {
        if (executorService != null) {
            terminate(executorService);
        }
    }

    public void testListenableFutureNotifiesListeners() {
        ListenableFuture<String> future = new ListenableFuture<>();
        AtomicInteger notifications = new AtomicInteger(0);
        final int numberOfListeners = scaledRandomIntBetween(1, 12);
        for (int i = 0; i < numberOfListeners; i++) {
            future.addListener(ActionListener.wrap(notifications::incrementAndGet), EsExecutors.newDirectExecutorService(), threadContext);
        }

        future.onResponse("");
        assertEquals(numberOfListeners, notifications.get());
        assertTrue(future.isDone());
    }

    public void testListenableFutureNotifiesListenersOnException() {
        ListenableFuture<String> future = new ListenableFuture<>();
        AtomicInteger notifications = new AtomicInteger(0);
        final int numberOfListeners = scaledRandomIntBetween(1, 12);
        final Exception exception = new RuntimeException();
        for (int i = 0; i < numberOfListeners; i++) {
            future.addListener(ActionListener.wrap(s -> fail("this should never be called"), e -> {
                assertEquals(exception, e);
                notifications.incrementAndGet();
            }), EsExecutors.newDirectExecutorService(), threadContext);
        }

        future.onFailure(exception);
        assertEquals(numberOfListeners, notifications.get());
        assertTrue(future.isDone());
    }

    public void testConcurrentListenerRegistrationAndCompletion() throws BrokenBarrierException, InterruptedException {
        final int numberOfThreads = scaledRandomIntBetween(2, 32);
        final int completingThread = randomIntBetween(0, numberOfThreads - 1);
        final ListenableFuture<String> future = new ListenableFuture<>();
        executorService = EsExecutors.newFixed("testConcurrentListenerRegistrationAndCompletion", numberOfThreads, 1000,
            EsExecutors.daemonThreadFactory("listener"), threadContext, false);
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        final CountDownLatch listenersLatch = new CountDownLatch(numberOfThreads - 1);
        final AtomicInteger numResponses = new AtomicInteger(0);
        final AtomicInteger numExceptions = new AtomicInteger(0);

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadNum = i;
            Thread thread = new Thread(() -> {
                threadContext.putTransient("key", threadNum);
                try {
                    barrier.await();
                    if (threadNum == completingThread) {
                        // we need to do more than just call onResponse as this often results in synchronous
                        // execution of the listeners instead of actually going async
                        final int waitTime = randomIntBetween(0, 50);
                        Thread.sleep(waitTime);
                        logger.info("completing the future after sleeping {}ms", waitTime);
                        future.onResponse("");
                        logger.info("future received response");
                    } else {
                        logger.info("adding listener {}", threadNum);
                        future.addListener(ActionListener.wrap(s -> {
                            logger.info("listener {} received value {}", threadNum, s);
                            assertEquals("", s);
                            assertThat(threadContext.getTransient("key"), is(threadNum));
                            numResponses.incrementAndGet();
                            listenersLatch.countDown();
                        }, e -> {
                            logger.error(new ParameterizedMessage("listener {} caught unexpected exception", threadNum), e);
                            numExceptions.incrementAndGet();
                            listenersLatch.countDown();
                        }), executorService, threadContext);
                        logger.info("listener {} added", threadNum);
                    }
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        barrier.await();
        barrier.await();
        listenersLatch.await();

        assertEquals(numberOfThreads - 1, numResponses.get());
        assertEquals(0, numExceptions.get());
    }
}
