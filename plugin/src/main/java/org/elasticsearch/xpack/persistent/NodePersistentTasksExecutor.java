/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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

    public <Request extends PersistentTaskRequest> void executeTask(Request request,
                                                                    AllocatedPersistentTask task,
                                                                    PersistentTasksExecutor<Request> action) {
        threadPool.executor(action.getExecutor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }

            @SuppressWarnings("unchecked")
            @Override
            protected void doRun() throws Exception {
                try {
                    action.nodeOperation(task, request);
                } catch (Exception ex) {
                    task.markAsFailed(ex);
                }

            }
        });

    }

}
