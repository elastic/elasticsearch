/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class ResponseHeadersCollectorTests extends ESTestCase {

    public void testCollect() {
        int numThreads = randomIntBetween(1, 10);
        TestThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "test", numThreads, 1024, "test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        Set<String> expectedWarnings = new HashSet<>();
        try {
            ThreadContext threadContext = threadPool.getThreadContext();
            var collector = new ResponseHeadersCollector(threadContext);
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            Runnable mergeAndVerify = () -> {
                collector.finish();
                List<String> actualWarnings = threadContext.getResponseHeaders().getOrDefault("Warnings", List.of());
                assertThat(Sets.newHashSet(actualWarnings), equalTo(expectedWarnings));
            };
            try (RefCountingListener refs = new RefCountingListener(ActionListener.runAfter(future, mergeAndVerify))) {
                CyclicBarrier barrier = new CyclicBarrier(numThreads);
                for (int i = 0; i < numThreads; i++) {
                    String warning = "warning-" + i;
                    expectedWarnings.add(warning);
                    ActionListener<Void> listener = ActionListener.runBefore(refs.acquire(), collector::collect);
                    threadPool.schedule(new ActionRunnable<>(listener) {
                        @Override
                        protected void doRun() throws Exception {
                            barrier.await(30, TimeUnit.SECONDS);
                            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                                threadContext.addResponseHeader("Warnings", warning);
                                listener.onResponse(null);
                            }
                        }
                    }, TimeValue.timeValueNanos(between(0, 1000_000)), threadPool.executor("test"));
                }
            }
            future.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            terminate(threadPool);
        }
    }
}
