/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.xpack.core.watcher.execution.Wid;

import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public interface WatchExecutor {

    BlockingQueue<Runnable> queue();

    Stream<Runnable> tasks();

    long largestPoolSize();

    void execute(Runnable runnable);

    void startTrace(Wid id);

    void stopTrace(Wid id);
}
