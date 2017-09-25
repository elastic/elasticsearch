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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        Collection<Integer> defaults = randomBoolean() ? Collections.singletonList(-1) :
            Collections.emptyList();
        GroupedActionListener<Integer> listener = new GroupedActionListener<>(result, groupSize,
            defaults);
        int numThreads = randomIntBetween(2, 5);
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread()  {
                @Override
                public void run() {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    int c = 0;
                    while((c = count.incrementAndGet()) <= groupSize) {
                        listener.onResponse(c-1);
                    }
                }
            };
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertNotNull(resRef.get());
        ArrayList<Integer> list = new ArrayList<>(resRef.get());
        Collections.sort(list);
        int expectedSize = groupSize + defaults.size();
        assertEquals(expectedSize, resRef.get().size());
        int expectedValue = defaults.isEmpty() ? 0 : -1;
        for (int i = 0; i < expectedSize; i++) {
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
        Collection<Integer> defaults = randomBoolean() ? Collections.singletonList(-1) :
            Collections.emptyList();
        int size = randomIntBetween(3, 4);
        GroupedActionListener<Integer> listener = new GroupedActionListener<>(result, size,
            defaults);
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
}
