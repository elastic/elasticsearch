/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * This component is responsible for execution of persistent tasks.
 *
 * It abstracts away the execution of tasks and greatly simplifies testing of PersistentTasksNodeService
 */
public class NodePersistentTasksExecutor {

    private final ThreadPool threadPool;

    NodePersistentTasksExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public <Params extends PersistentTaskParams> void executeTask(
        final Params params,
        final @Nullable PersistentTaskState state,
        final AllocatedPersistentTask task,
        final PersistentTasksExecutor<Params> executor
    ) {
        threadPool.executor(executor.getExecutor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }

            @Override
            protected void doRun() throws Exception {
                try {
                    executor.nodeOperation(task, params, state);
                } catch (Exception ex) {
                    task.markAsFailed(ex);
                }

            }
        });
    }
}
