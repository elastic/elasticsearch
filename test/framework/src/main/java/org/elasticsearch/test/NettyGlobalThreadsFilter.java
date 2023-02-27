/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Excludes threads started by {@link io.netty.util.concurrent.GlobalEventExecutor} which are static per-JVM and reused across test suits.
 * We make sure we don't leak any tasks on them in {@link ESIntegTestCase#awaitGlobalNettyThreadsFinish()}.
 */
public class NettyGlobalThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("globalEventExecutor");
    }
}
