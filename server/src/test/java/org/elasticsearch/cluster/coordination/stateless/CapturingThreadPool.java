/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayDeque;
import java.util.Deque;

class CapturingThreadPool extends TestThreadPool {
    final Deque<Tuple<TimeValue, Runnable>> scheduledTasks = new ArrayDeque<>();

    CapturingThreadPool(String name) {
        super(name);
    }

    @Override
    public ScheduledCancellable schedule(Runnable task, TimeValue delay, String executor) {
        scheduledTasks.add(new Tuple<>(delay, task));
        return null;
    }
}
