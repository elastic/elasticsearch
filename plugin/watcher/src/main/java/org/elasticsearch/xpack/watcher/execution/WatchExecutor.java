/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public interface WatchExecutor {

    BlockingQueue<Runnable> queue();

    Stream<Runnable> tasks();

    long largestPoolSize();

    void execute(Runnable runnable);

}
