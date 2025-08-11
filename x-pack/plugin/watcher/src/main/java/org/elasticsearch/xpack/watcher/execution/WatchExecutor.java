/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import java.util.stream.Stream;

public interface WatchExecutor {

    int getCurrentQueueSize();

    int drainQueue();

    Stream<Runnable> tasks();

    long largestPoolSize();

    void execute(Runnable runnable);

}
