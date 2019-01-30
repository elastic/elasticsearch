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
package org.elasticsearch.transport;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;

public class TransportRequestDeduplicatorTests extends ESTestCase {

    public void testRequestDeduplication() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        Exception failure = randomBoolean() ? new TransportException("simulated") : null;
        final TransportRequest request = new TransportRequest() {
            @Override
            public void setParentTask(final TaskId taskId) {
            }
        };
        final TransportRequestDeduplicator<TransportRequest> deduplicator = new TransportRequestDeduplicator<>();
        final SetOnce<ActionListener<Void>> listenerHolder = new SetOnce<>();
        int iterationsPerThread = scaledRandomIntBetween(100, 1000);
        Thread[] threads = new Thread[between(1, 4)];
        Phaser barrier = new Phaser(threads.length + 1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                barrier.arriveAndAwaitAdvance();
                for (int n = 0; n < iterationsPerThread; n++) {
                    deduplicator.executeOnce(request, new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            successCount.incrementAndGet();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(e, sameInstance(failure));
                            failureCount.incrementAndGet();
                        }
                    }, (req, reqListener) -> listenerHolder.set(reqListener));
                }
            });
            threads[i].start();
        }
        barrier.arriveAndAwaitAdvance();
        for (Thread t : threads) {
            t.join();
        }
        final ActionListener<Void> listener = listenerHolder.get();
        assertThat(deduplicator.size(), equalTo(1));
        if (failure != null) {
            listener.onFailure(failure);
        } else {
            listener.onResponse(null);
        }
        assertThat(deduplicator.size(), equalTo(0));
        assertBusy(() -> {
            if (failure != null) {
                assertThat(successCount.get(), equalTo(0));
                assertThat(failureCount.get(), equalTo(threads.length * iterationsPerThread));
            } else {
                assertThat(successCount.get(), equalTo(threads.length * iterationsPerThread));
                assertThat(failureCount.get(), equalTo(0));
            }
        });
    }

}
