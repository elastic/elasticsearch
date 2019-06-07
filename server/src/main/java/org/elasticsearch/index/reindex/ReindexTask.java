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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ReindexTask extends AllocatedPersistentTask {

    // TODO: Name
    public static final String NAME = "reindex/job";

    private final AtomicReference<ReindexJob> taskState = new AtomicReference<>();
    private final Client client;

    public static class ReindexPersistentTasksExecutor extends PersistentTasksExecutor<ReindexJob> {

        private final Client client;

        protected ReindexPersistentTasksExecutor(final Client client) {
            super(NAME, ThreadPool.Names.GENERIC);
            this.client = client;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, ReindexJob reindexJob, PersistentTaskState state) {
            ReindexTask reindexTask = (ReindexTask) task;
            reindexTask.doReindex();

        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTasksCustomMetaData.PersistentTask<ReindexJob> taskInProgress,
                                                     Map<String, String> headers) {
            return new ReindexTask(id, type, action, parentTaskId, headers, taskInProgress.getParams(), client);
        }
    }

    private ReindexTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers, ReindexJob reindexJob,
                        Client client) {
        super(id, type, action, "reindex_" + id, parentTask, headers);
        taskState.set(reindexJob);
        this.client = client;
    }

    private void doReindex() {
        client.execute(ReindexAction.INSTANCE, taskState.get().getReindexRequest(), new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse response) {
                updatePersistentTaskState(null, new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {

                    }

                    @Override
                    public void onFailure(Exception e) {

                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                updatePersistentTaskState(null, new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {

                    }

                    @Override
                    public void onFailure(Exception e) {

                    }
                });
            }
        });
    }
}
