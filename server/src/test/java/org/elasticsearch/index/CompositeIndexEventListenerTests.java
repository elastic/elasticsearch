/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CompositeIndexEventListenerTests extends IndexShardTestCase {

    private Exception getRootCause(Exception e) {
        while (e.getCause() instanceof Exception ee) {
            e = ee;
        }
        if (e.getCause() != null) {
            throw new AssertionError(e.getCause());
        }
        return e;
    }

    public void testBeforeIndexShardRecoveryInOrder() throws Exception {
        var shard = newShard(randomBoolean());
        var appender = new MockLogAppender();
        try (var ignored = appender.capturing(CompositeIndexEventListener.class)) {
            final var stepNumber = new AtomicInteger();
            final var stepCount = between(0, 20);
            final var failAtStep = new AtomicInteger(-1);
            final var indexEventListener = new CompositeIndexEventListener(
                shard.indexSettings(),
                IntStream.range(0, stepCount).mapToObj(step -> new IndexEventListener() {
                    @Override
                    public void beforeIndexShardRecovery(
                        IndexShard indexShard,
                        IndexSettings indexSettings,
                        ActionListener<Void> listener
                    ) {
                        if (randomBoolean()) {
                            // throws an exception sometimes
                            runStep();
                            listener.onResponse(null);
                        } else {
                            // fails the listener sometimes
                            shard.getThreadPool()
                                .executor(randomFrom(ThreadPool.Names.GENERIC, ThreadPool.Names.SAME))
                                .execute(ActionRunnable.run(listener, this::runStep));
                        }
                    }

                    private void runStep() {
                        assertThat(step, Matchers.lessThanOrEqualTo(failAtStep.get()));
                        assertTrue(stepNumber.compareAndSet(step, step + 1));
                        if (step == failAtStep.get()) {
                            throw new ElasticsearchException("simulated failure at step " + step);
                        }
                    }
                }).collect(Collectors.toList())
            );

            final CheckedRunnable<Exception> beforeIndexShardRecoveryRunner = () -> assertNull(
                PlainActionFuture.<Void, Exception>get(
                    fut -> indexEventListener.beforeIndexShardRecovery(shard, shard.indexSettings(), fut),
                    10,
                    TimeUnit.SECONDS
                )
            );

            failAtStep.set(stepCount);
            beforeIndexShardRecoveryRunner.run();
            assertEquals(stepCount, stepNumber.getAndSet(0));

            if (stepCount > 0) {
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "warning",
                        CompositeIndexEventListener.class.getCanonicalName(),
                        Level.WARN,
                        "*failed to invoke the listener before the shard recovery starts for [index][0]"
                    )
                );

                failAtStep.set(between(0, stepCount - 1));
                final var rootCause = getRootCause(expectThrows(ElasticsearchException.class, beforeIndexShardRecoveryRunner::run));
                assertEquals("simulated failure at step " + failAtStep.get(), rootCause.getMessage());
                assertEquals(failAtStep.get() + 1, stepNumber.getAndSet(0));
                appender.assertAllExpectationsMatched();
            }

        } finally {
            closeShards(shard);
        }
    }
}
