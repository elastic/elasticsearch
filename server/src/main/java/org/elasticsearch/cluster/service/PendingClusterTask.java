/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.PENDING_CLUSTER_TASKS_DETAILS_ADDED;

public record PendingClusterTask(
    long insertOrder,
    Priority priority,
    Text source,
    String queueName,
    String taskDescription,
    long timeInQueue,
    boolean executing
) implements Writeable {

    public PendingClusterTask(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            Priority.readFrom(in),
            readSource(in),
            readQueueName(in),
            readDescription(in),
            readTimeInQueue(in),
            in.readBoolean()
        );
    }

    private static Text readSource(StreamInput in) throws IOException {
        return in.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED) ? new Text(in.readString()) : in.readText();
    }

    private static String readQueueName(StreamInput in) throws IOException {
        return in.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED) ? in.readString() : "";
    }

    private static String readDescription(StreamInput in) throws IOException {
        return in.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED) ? in.readOptionalString() : null;
    }

    private static long readTimeInQueue(StreamInput in) throws IOException {
        return in.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED) ? in.readVLong() : in.readLong();
    }

    public PendingClusterTask {
        assert timeInQueue >= 0 : "got a negative timeInQueue [" + timeInQueue + "]";
        assert insertOrder >= 0 : "got a negative insertOrder [" + insertOrder + "]";
        assert source.hasString();
    }

    public long getInsertOrder() {
        return insertOrder;
    }

    public Priority getPriority() {
        return priority;
    }

    public Text getSource() {
        return source;
    }

    public long getTimeInQueueInMillis() {
        return timeInQueue;
    }

    public TimeValue getTimeInQueue() {
        return new TimeValue(getTimeInQueueInMillis());
    }

    public boolean isExecuting() {
        return executing;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(insertOrder);
        Priority.writeTo(priority, out);
        if (out.getTransportVersion().onOrAfter(PENDING_CLUSTER_TASKS_DETAILS_ADDED)) {
            out.writeString(source.string());
            out.writeString(queueName);
            out.writeOptionalString(taskDescription);
            out.writeVLong(timeInQueue);
        } else {
            out.writeText(source);
            // earlier versions don't support detailed mode, can just omit the queue name and task description
            out.writeLong(timeInQueue);
        }
        out.writeBoolean(executing);
    }
}
