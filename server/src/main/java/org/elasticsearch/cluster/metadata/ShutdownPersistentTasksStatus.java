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
 * ({@code autoReassignableTasksRemaining}), and an overall {@code status} that is {@code COMPLETE} when no auto
 * reassigned tasks remain or {@code IN_PROGRESS} otherwise.
 * If the node is not present and has not been seen in the cluster, the status will be {@code NOT_STARTED}.
 */
public class ShutdownPersistentTasksStatus implements Writeable, ToXContentObject {

    public static final TransportVersion SHUTDOWN_PERSISTENT_TASKS_STATUS = TransportVersion.fromName("shutdown_persistent_tasks_status");

    private static final ShutdownPersistentTasksStatus NOT_STARTED_STATUS = new ShutdownPersistentTasksStatus(
        SingleNodeShutdownMetadata.Status.NOT_STARTED,
        0,
        0
    );

    private final SingleNodeShutdownMetadata.Status status;
    private final int persistentTasksRemaining;
    private final int autoReassignableTasksRemaining;

    public static ShutdownPersistentTasksStatus notStarted() {
        return NOT_STARTED_STATUS;
    }

    public static ShutdownPersistentTasksStatus fromRemainingTasks(int persistentTasksRemaining, int autoReassignableTasksRemaining) {
        // Only auto-reassigned persistent tasks will be proactively relocated in response to node shutdown.
        // Other persistent tasks either handle their own abort or wait to be unassigned then assigned
        // once the node finishes shutdown.
        SingleNodeShutdownMetadata.Status status = autoReassignableTasksRemaining == 0
            ? SingleNodeShutdownMetadata.Status.COMPLETE
            : SingleNodeShutdownMetadata.Status.IN_PROGRESS;
        return new ShutdownPersistentTasksStatus(status, persistentTasksRemaining, autoReassignableTasksRemaining);
    }

    private ShutdownPersistentTasksStatus(
        SingleNodeShutdownMetadata.Status status,
        int persistentTasksRemaining,
        int autoReassignableTasksRemaining
    ) {
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.persistentTasksRemaining = persistentTasksRemaining;
        this.autoReassignableTasksRemaining = autoReassignableTasksRemaining;
        assertValidStatus(status, persistentTasksRemaining, autoReassignableTasksRemaining);
    }

    public ShutdownPersistentTasksStatus(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(SHUTDOWN_PERSISTENT_TASKS_STATUS)) {
            this.status = in.readEnum(SingleNodeShutdownMetadata.Status.class);
            this.persistentTasksRemaining = in.readVInt();
            this.autoReassignableTasksRemaining = in.readVInt();
        } else {
            this.status = SingleNodeShutdownMetadata.Status.COMPLETE;
            this.persistentTasksRemaining = 0;
            this.autoReassignableTasksRemaining = 0;
        }
        assertValidStatus(this.status, this.persistentTasksRemaining, this.autoReassignableTasksRemaining);
    }

    private static void assertValidStatus(
        SingleNodeShutdownMetadata.Status status,
        int persistentTasksRemaining,
        int autoReassignableTasksRemaining
    ) {
        assert autoReassignableTasksRemaining >= 0 && autoReassignableTasksRemaining <= persistentTasksRemaining
            : "autoReassignableTasksRemaining ["
                + autoReassignableTasksRemaining
                + "] must be >= 0 and <= persistentTasksRemaining ["
                + persistentTasksRemaining
                + "]";
        assert status != SingleNodeShutdownMetadata.Status.COMPLETE || autoReassignableTasksRemaining == 0
            : "status cannot be complete if autoReassignableTasksRemaining > 0";
        assert status != SingleNodeShutdownMetadata.Status.NOT_STARTED
            || (autoReassignableTasksRemaining == 0 && persistentTasksRemaining == 0)
            : "status cannot be not_started if autoReassignableTasksRemaining != 0 or persistentTasksRemaining != 0";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(SHUTDOWN_PERSISTENT_TASKS_STATUS)) {
            out.writeEnum(status);
            out.writeVInt(persistentTasksRemaining);
            out.writeVInt(autoReassignableTasksRemaining);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.field("persistent_tasks_remaining", persistentTasksRemaining);
        builder.field("auto_reassignable_tasks_remaining", autoReassignableTasksRemaining);
        builder.endObject();
        return builder;
    }

    public SingleNodeShutdownMetadata.Status getStatus() {
        return status;
    }

    public int getPersistentTasksRemaining() {
        return persistentTasksRemaining;
    }

    public int getAutoReassignableTasksRemaining() {
        return autoReassignableTasksRemaining;
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, persistentTasksRemaining, autoReassignableTasksRemaining);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShutdownPersistentTasksStatus other = (ShutdownPersistentTasksStatus) o;
        return status.equals(other.status)
            && persistentTasksRemaining == other.persistentTasksRemaining
            && autoReassignableTasksRemaining == other.autoReassignableTasksRemaining;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
