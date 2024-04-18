/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.concurrent.Phaser;

/**
 * Static utility methods for testing system index thread pools
 *
 * <p>Some of our plugins already have large and well-developed integration test cases,
 * and can't also subclass {@link SystemIndexThreadPoolTestCase}. Integration tests for
 * such plugins can call {@link #runWithBlockedThreadPools(Collection, InternalTestCluster, Runnable)}
 * directly.</p>
 */
public class SystemIndexThreadPoolUtils {

    private SystemIndexThreadPoolUtils() {
        throw new AssertionError("This is a utility class and shouldn't be instantiated");
    }

    public static void runWithBlockedThreadPools(
        Collection<String> threadPoolsToBlock,
        InternalTestCluster internalCluster,
        Runnable runnable
    ) {
        Phaser phaser = new Phaser();
        Runnable waitAction = () -> {
            phaser.arriveAndAwaitAdvance();
            phaser.arriveAndAwaitAdvance();
        };
        phaser.register(); // register this test's thread

        for (String nodeName : internalCluster.getNodeNames()) {
            ThreadPool threadPool = internalCluster.getInstance(ThreadPool.class, nodeName);
            for (String threadPoolName : threadPoolsToBlock) {
                ThreadPool.Info info = threadPool.info(threadPoolName);
                phaser.bulkRegister(info.getMax());
                for (int i = 0; i < info.getMax(); i++) {
                    threadPool.executor(threadPoolName).submit(waitAction);
                }
            }
        }
        phaser.arriveAndAwaitAdvance();
        try {
            runnable.run();
        } finally {
            phaser.arriveAndAwaitAdvance();
        }
    }
}
