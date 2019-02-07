/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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

    public <Params extends PersistentTaskParams> void executeTask(final Params params,
                                                                  final @Nullable PersistentTaskState state,
                                                                  final AllocatedPersistentTask task,
                                                                  final PersistentTasksExecutor<Params> executor) {
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
