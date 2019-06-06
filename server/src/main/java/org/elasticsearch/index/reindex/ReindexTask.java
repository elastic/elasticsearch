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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

public class ReindexTask extends AllocatedPersistentTask {

    private final ReindexJob reindexJob;

    public class ReindexJob implements PersistentTaskParams {

        // TODO: Name
        public static final String NAME = "reindex";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            // TODO: version
            return Version.V_8_0_0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }

    public static class ReindexPersistentTasksExecutor extends PersistentTasksExecutor<ReindexJob> {

        protected ReindexPersistentTasksExecutor() {
            // TODO: Name
            super("reindex_task", ThreadPool.Names.GENERIC);
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
            return new ReindexTask(id, type, action, parentTaskId, headers, taskInProgress.getParams());
        }
    }

    ReindexTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers, ReindexJob reindexJob) {
        super(id, type, action, "reindex_" + id, parentTask, headers);
        this.reindexJob = reindexJob;
    }

    private void doReindex() {

    }
}
