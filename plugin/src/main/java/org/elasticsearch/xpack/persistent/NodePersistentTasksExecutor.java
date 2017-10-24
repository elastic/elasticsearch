/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * This component is responsible for execution of persistent tasks.
 *
 * It abstracts away the execution of tasks and greatly simplifies testing of PersistentTasksNodeService
 */
public class NodePersistentTasksExecutor {
    private final ThreadPool threadPool;

    public NodePersistentTasksExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public <Params extends PersistentTaskParams> void executeTask(@Nullable Params params,
                                                                  @Nullable Task.Status status,
                                                                  AllocatedPersistentTask task,
                                                                  PersistentTasksExecutor<Params> executor) {
        threadPool.executor(executor.getExecutor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }

            @SuppressWarnings("unchecked")
            @Override
            protected void doRun() throws Exception {
                try {
                    executor.nodeOperation(task, params, status);
                } catch (Exception ex) {
                    task.markAsFailed(ex);
                }

            }
        });

    }

}
