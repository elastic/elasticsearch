/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.Executor;

public class ThrottledTaskRunner extends AbstractThrottledTaskRunner<ActionListener<Releasable>> {
    // a simple AbstractThrottledTaskRunner which fixes the task type and uses a regular FIFO blocking queue.
    public ThrottledTaskRunner(String name, int maxRunningTasks, Executor executor) {
        super(name, maxRunningTasks, executor, ConcurrentCollections.newBlockingQueue());
    }
}
