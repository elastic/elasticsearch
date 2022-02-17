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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
            public void setParentTask(final TaskId taskId) {}
        };
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ResultDeduplicator<TransportRequest, Void> deduplicator = new ResultDeduplicator<>(threadContext);
        final SetOnce<ActionListener<Void>> listenerHolder = new SetOnce<>();
        final var headerName = "thread-context-header";
        final var headerGenerator = new AtomicInteger();
        int iterationsPerThread = scaledRandomIntBetween(100, 1000);
        Thread[] threads = new Thread[between(1, 4)];
        Phaser barrier = new Phaser(threads.length + 1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                barrier.arriveAndAwaitAdvance();
                for (int n = 0; n < iterationsPerThread; n++) {
                    final var headerValue = Integer.toString(headerGenerator.incrementAndGet());
                    try (var ignored = threadContext.stashContext()) {
                        threadContext.putHeader(headerName, headerValue);
                        deduplicator.executeOnce(request, new ActionListener<>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                assertThat(threadContext.getHeader(headerName), equalTo(headerValue));
                                successCount.incrementAndGet();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                assertThat(threadContext.getHeader(headerName), equalTo(headerValue));
                                assertThat(e, sameInstance(failure));
                                failureCount.incrementAndGet();
                            }
                        }, (req, reqListener) -> listenerHolder.set(reqListener));
                    }
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
