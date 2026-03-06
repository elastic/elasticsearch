/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Tracks the status of persistent tasks on a node that is shutting down.
 * <p>
 * Reports the total number of persistent tasks still assigned to the shutdown node ({@code persistentTasksRemaining}),
 * the subset of those that will be proactively reassigned by the persistent task framework
 * ({@code autoReassignedTasksRemaining}), and an overall {@code status} that is {@code COMPLETE} when no auto
 * reassigned tasks remain or {@code IN_PROGRESS} otherwise.
 * If the node is not present and has not been seen in the cluster, the status will be {@code NOT_STARTED}.
 */
public class ShutdownPersistentTasksStatus implements Writeable, ToXContentObject {

    public static final TransportVersion SHUTDOWN_PERSISTENT_TASKS_STATUS = TransportVersion.fromName("shutdown_persistent_tasks_status");

    private final SingleNodeShutdownMetadata.Status status;
    private final int persistentTasksRemaining;
    private final int autoReassignedTasksRemaining;

    public ShutdownPersistentTasksStatus(
        SingleNodeShutdownMetadata.Status status,
        int persistentTasksRemaining,
        int autoReassignedTasksRemaining
    ) {
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.persistentTasksRemaining = persistentTasksRemaining;
        this.autoReassignedTasksRemaining = autoReassignedTasksRemaining;
    }

    public ShutdownPersistentTasksStatus(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(SHUTDOWN_PERSISTENT_TASKS_STATUS)) {
            this.status = in.readEnum(SingleNodeShutdownMetadata.Status.class);
            this.persistentTasksRemaining = in.readVInt();
            this.autoReassignedTasksRemaining = in.readVInt();
        } else {
            this.status = SingleNodeShutdownMetadata.Status.COMPLETE;
            this.persistentTasksRemaining = 0;
            this.autoReassignedTasksRemaining = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(SHUTDOWN_PERSISTENT_TASKS_STATUS)) {
            out.writeEnum(status);
            out.writeVInt(persistentTasksRemaining);
            out.writeVInt(autoReassignedTasksRemaining);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.field("persistent_tasks_remaining", persistentTasksRemaining);
        builder.field("auto_reassigned_tasks_remaining", autoReassignedTasksRemaining);
        builder.endObject();
        return builder;
    }

    public SingleNodeShutdownMetadata.Status getStatus() {
        return status;
    }

    public int getPersistentTasksRemaining() {
        return persistentTasksRemaining;
    }

    public int getAutoReassignedTasksRemaining() {
        return autoReassignedTasksRemaining;
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, persistentTasksRemaining, autoReassignedTasksRemaining);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShutdownPersistentTasksStatus other = (ShutdownPersistentTasksStatus) o;
        return status.equals(other.status)
            && persistentTasksRemaining == other.persistentTasksRemaining
            && autoReassignedTasksRemaining == other.autoReassignedTasksRemaining;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
