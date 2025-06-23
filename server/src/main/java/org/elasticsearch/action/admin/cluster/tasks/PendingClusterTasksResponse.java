/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PendingClusterTasksResponse extends ActionResponse implements ChunkedToXContentObject {

    private final List<PendingClusterTask> pendingTasks;

    public PendingClusterTasksResponse(StreamInput in) throws IOException {
        pendingTasks = in.readCollectionAsList(PendingClusterTask::new);
    }

    PendingClusterTasksResponse(List<PendingClusterTask> pendingTasks) {
        this.pendingTasks = pendingTasks;
    }

    public List<PendingClusterTask> pendingTasks() {
        return pendingTasks;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tasks: (").append(pendingTasks.size()).append("):\n");
        for (PendingClusterTask pendingClusterTask : pendingTasks) {
            sb.append(pendingClusterTask.getInsertOrder())
                .append("/")
                .append(pendingClusterTask.getPriority())
                .append("/")
                .append(pendingClusterTask.getSource())
                .append("/")
                .append(pendingClusterTask.getTimeInQueue())
                .append("\n");
        }
        return sb.toString();
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((builder, p) -> {
            builder.startObject();
            builder.startArray(Fields.TASKS);
            return builder;
        }), Iterators.map(pendingTasks.iterator(), pendingClusterTask -> (builder, p) -> {
            builder.startObject();
            builder.field(Fields.INSERT_ORDER, pendingClusterTask.getInsertOrder());
            builder.field(Fields.PRIORITY, pendingClusterTask.getPriority());
            builder.field(Fields.SOURCE, pendingClusterTask.getSource());
            builder.field(Fields.EXECUTING, pendingClusterTask.isExecuting());
            builder.field(Fields.TIME_IN_QUEUE_MILLIS, pendingClusterTask.getTimeInQueueInMillis());
            builder.field(Fields.TIME_IN_QUEUE, pendingClusterTask.getTimeInQueue());
            builder.endObject();
            return builder;
        }), Iterators.single((builder, p) -> {
            builder.endArray();
            builder.endObject();
            return builder;
        }));
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
        out.writeCollection(pendingTasks);
    }

}
