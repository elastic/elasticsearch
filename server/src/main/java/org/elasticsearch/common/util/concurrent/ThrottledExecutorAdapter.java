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

import static org.elasticsearch.common.Strings.format;

public class ThrottledExecutorAdapter implements Executor {
    private final Logger logger = LogManager.getLogger(ThrottledExecutorAdapter.class);

    private final ThrottledTaskRunner throttledTaskRunner;

    public ThrottledExecutorAdapter(ThrottledTaskRunner throttledTaskRunner) {
        this.throttledTaskRunner = throttledTaskRunner;
    }

    @Override
    public void execute(Runnable task) {
        throttledTaskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    task.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> format(
                        "[%s] failed to execute task %s by executor [%s]",
                        throttledTaskRunner.getTaskRunnerName(),
                        task,
                        ThrottledExecutorAdapter.class.getCanonicalName()
                    ),
                    e
                );
            }

            @Override
            public String toString() {
                return task.toString();
            }
        });
    }
}
