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

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PendingClusterTasksResponse extends ActionResponse implements Iterable<PendingClusterTask>, ToXContentObject {

    private List<PendingClusterTask> pendingTasks;

    public PendingClusterTasksResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        pendingTasks = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            pendingTasks.add(new PendingClusterTask(in));
        }
    }

    PendingClusterTasksResponse(List<PendingClusterTask> pendingTasks) {
        this.pendingTasks = pendingTasks;
    }

    public List<PendingClusterTask> pendingTasks() {
        return pendingTasks;
    }

    /**
     * The pending cluster tasks
     */
    public List<PendingClusterTask> getPendingTasks() {
        return pendingTasks();
    }

    @Override
    public Iterator<PendingClusterTask> iterator() {
        return pendingTasks.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tasks: (").append(pendingTasks.size()).append("):\n");
        for (PendingClusterTask pendingClusterTask : this) {
            sb.append(pendingClusterTask.getInsertOrder()).append("/").append(pendingClusterTask.getPriority()).append("/")
                    .append(pendingClusterTask.getSource()).append("/").append(pendingClusterTask.getTimeInQueue()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.TASKS);
        for (PendingClusterTask pendingClusterTask : this) {
            builder.startObject();
            builder.field(Fields.INSERT_ORDER, pendingClusterTask.getInsertOrder());
            builder.field(Fields.PRIORITY, pendingClusterTask.getPriority());
            builder.field(Fields.SOURCE, pendingClusterTask.getSource());
            builder.field(Fields.EXECUTING, pendingClusterTask.isExecuting());
            builder.field(Fields.TIME_IN_QUEUE_MILLIS, pendingClusterTask.getTimeInQueueInMillis());
            builder.field(Fields.TIME_IN_QUEUE, pendingClusterTask.getTimeInQueue());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    static final class Fields {

        static final String TASKS = "tasks";
        static final String EXECUTING = "executing";
        static final String INSERT_ORDER = "insert_order";
        static final String PRIORITY = "priority";
        static final String SOURCE = "source";
        static final String TIME_IN_QUEUE_MILLIS = "time_in_queue_millis";
        static final String TIME_IN_QUEUE = "time_in_queue";

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pendingTasks.size());
        for (PendingClusterTask task : pendingTasks) {
            task.writeTo(out);
        }
    }

}
