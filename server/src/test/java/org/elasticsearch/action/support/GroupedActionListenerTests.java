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
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GroupedActionListenerTests extends ESTestCase {

    public void testNotifications() throws InterruptedException {
        AtomicReference<Collection<Integer>> resRef = new AtomicReference<>();
        ActionListener<Collection<Integer>> result = new ActionListener<Collection<Integer>>() {
            @Override
            public void onResponse(Collection<Integer> integers) {
                resRef.set(integers);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
        final int groupSize = randomIntBetween(10, 1000);
        AtomicInteger count = new AtomicInteger();
        GroupedActionListener<Integer> listener = new GroupedActionListener<>(result, groupSize);
        int numThreads = randomIntBetween(2, 5);
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                int c;
                while((c = count.incrementAndGet()) <= groupSize) {
                    listener.onResponse(c-1);
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertNotNull(resRef.get());
        ArrayList<Integer> list = new ArrayList<>(resRef.get());
        Collections.sort(list);
        assertEquals(groupSize, resRef.get().size());
        int expectedValue = 0;
        for (int i = 0; i < groupSize; i++) {
            assertEquals(Integer.valueOf(expectedValue++), list.get(i));
        }
    }

    public void testFailed() {
        AtomicReference<Collection<Integer>> resRef = new AtomicReference<>();
        AtomicReference<Exception> excRef = new AtomicReference<>();

        ActionListener<Collection<Integer>> result = new ActionListener<Collection<Integer>>() {
            @Override
            public void onResponse(Collection<Integer> integers) {
                resRef.set(integers);
            }

            @Override
            public void onFailure(Exception e) {
                excRef.set(e);
            }
        };
        int size = randomIntBetween(3, 4);
        GroupedActionListener<Integer> listener = new GroupedActionListener<>(result, size);
        listener.onResponse(0);
        IOException ioException = new IOException();
        RuntimeException rtException = new RuntimeException();
        listener.onFailure(rtException);
        listener.onFailure(ioException);
        if (size == 4) {
            listener.onResponse(2);
        }
        assertNotNull(excRef.get());
        assertEquals(rtException, excRef.get());
        assertEquals(1, excRef.get().getSuppressed().length);
        assertEquals(ioException, excRef.get().getSuppressed()[0]);
        assertNull(resRef.get());
        listener.onResponse(1);
        assertNull(resRef.get());
    }

    public void testConcurrentFailures() throws InterruptedException {
        AtomicReference<Exception> finalException = new AtomicReference<>();
        int numGroups = randomIntBetween(10, 100);
        GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.wrap(r -> {}, finalException::set), numGroups);
        ExecutorService executorService = Executors.newFixedThreadPool(numGroups);
        for (int i = 0; i < numGroups; i++) {
            executorService.submit(() -> listener.onFailure(new IOException()));
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        Exception exception = finalException.get();
        assertNotNull(exception);
        assertThat(exception, instanceOf(IOException.class));
        assertEquals(numGroups - 1, exception.getSuppressed().length);
    }

    /*
     * It can happen that the same exception causes a grouped listener to be notified of the failure multiple times. Since we suppress
     * additional exceptions into the first exception, we have to guard against suppressing into the same exception, which could occur if we
     * are notified of with the same failure multiple times. This test verifies that the guard against self-suppression remains.
     */
    public void testRepeatNotificationForTheSameException() {
        final AtomicReference<Exception> finalException = new AtomicReference<>();
        final GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.wrap(r -> {}, finalException::set), 2);
        final Exception e = new Exception();
        // repeat notification for the same exception
        listener.onFailure(e);
        listener.onFailure(e);
        assertThat(finalException.get(), not(nullValue()));
        assertThat(finalException.get(), equalTo(e));
    }

}
