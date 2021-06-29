/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class ESThreadPoolTestCase extends ESTestCase {

    protected final ThreadPool.Info info(final ThreadPool threadPool, final String name) {
        for (final ThreadPool.Info info : threadPool.info()) {
            if (info.getName().equals(name)) {
                return info;
            }
        }
        assert "same".equals(name);
        return null;
    }

    protected final ThreadPoolStats.Stats stats(final ThreadPool threadPool, final String name) {
        for (final ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (name.equals(stats.getName())) {
                return stats;
            }
        }
        throw new IllegalArgumentException(name);
    }

    protected final void terminateThreadPoolIfNeeded(final ThreadPool threadPool) throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    static String randomThreadPool(final ThreadPool.ThreadPoolType type) {
        return randomFrom(
                ThreadPool.THREAD_POOL_TYPES
                        .entrySet().stream()
                        .filter(t -> t.getValue().equals(type))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList()));
    }

}
