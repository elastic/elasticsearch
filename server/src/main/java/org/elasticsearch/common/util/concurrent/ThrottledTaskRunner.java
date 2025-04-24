/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.Executor;

public class ThrottledTaskRunner extends AbstractThrottledTaskRunner<ActionListener<Releasable>> {

    private static final Logger logger = LogManager.getLogger(ThrottledTaskRunner.class);

    // a simple AbstractThrottledTaskRunner which fixes the task type and uses a regular FIFO blocking queue.
    public ThrottledTaskRunner(String name, int maxRunningTasks, Executor executor) {
        super(name, maxRunningTasks, executor, ConcurrentCollections.newBlockingQueue());
    }

    public static Executor buildSingleThreadedExecutor(String name, Executor executor) {
        final ThrottledTaskRunner throttledTaskRunner = new ThrottledTaskRunner(name, 1, executor);
        return r -> throttledTaskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    r.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (r instanceof AbstractRunnable abstractRunnable) {
                    abstractRunnable.onFailure(e);
                } else {
                    // should be impossible, we should always submit an AbstractRunnable
                    logger.error("unexpected failure running [" + r + "] on [" + name + "]", e);
                    assert false : new AssertionError("unexpected failure running " + r, e);
                }
            }
        });
    }
}
