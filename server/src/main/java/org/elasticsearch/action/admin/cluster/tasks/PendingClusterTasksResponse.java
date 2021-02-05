/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PendingClusterTasksResponse extends ActionResponse implements Iterable<PendingClusterTask>, ToXContentObject {

    private final List<PendingClusterTask> pendingTasks;

    public PendingClusterTasksResponse(StreamInput in) throws IOException {
        super(in);
        pendingTasks = in.readList(PendingClusterTask::new);
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
        out.writeList(pendingTasks);
    }

}
