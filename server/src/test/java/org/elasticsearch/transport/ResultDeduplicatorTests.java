/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;

public class ResultDeduplicatorTests extends ESTestCase {

    public void testRequestDeduplication() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        Exception failure = randomBoolean() ? new TransportException("simulated") : null;
        final TransportRequest request = new TransportRequest() {
            @Override
            public void setParentTask(final TaskId taskId) {
            }
        };
        final ResultDeduplicator<TransportRequest, Void> deduplicator = new ResultDeduplicator<>();
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
