/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.persistent.PersistentTasks.Parsers.PERSISTENT_TASK_PARSER;

/**
 * A cluster state record that contains a list of all running persistent tasks for the cluster itself
 */
public final class ClusterPersistentTasksCustomMetadata extends AbstractNamedDiffable<Metadata.ClusterCustom>
    implements
        Metadata.ClusterCustom,
        PersistentTasks {

    public static final String TYPE = "cluster_persistent_tasks";

    static final ObjectParser<Builder, Void> PERSISTENT_TASKS_PARSER = new ObjectParser<>(TYPE, Builder::new);

    static {
        // Tasks parser initialization
        PERSISTENT_TASKS_PARSER.declareLong(Builder::setLastAllocationId, new ParseField("last_allocation_id"));
        PERSISTENT_TASKS_PARSER.declareObjectArray(Builder::setTasks, PERSISTENT_TASK_PARSER, new ParseField("tasks"));
    }

    @Deprecated(forRemoval = true)
    public static ClusterPersistentTasksCustomMetadata getPersistentTasksCustomMetadata(ClusterState clusterState) {
        return get(clusterState.metadata());
    }

    public static ClusterPersistentTasksCustomMetadata get(Metadata metadata) {
        return metadata.custom(TYPE);
    }

    // TODO: Implement custom Diff for tasks
    private final Map<String, PersistentTasksCustomMetadata.PersistentTask<?>> tasks;
    private final long lastAllocationId;

    public ClusterPersistentTasksCustomMetadata(long lastAllocationId, Map<String, PersistentTasksCustomMetadata.PersistentTask<?>> tasks) {
        this.lastAllocationId = lastAllocationId;
        this.tasks = tasks;
    }

    public ClusterPersistentTasksCustomMetadata(StreamInput in) throws IOException {
        lastAllocationId = in.readLong();
        tasks = in.readMap(PersistentTasksCustomMetadata.PersistentTask::new);
    }

    public static ClusterPersistentTasksCustomMetadata fromXContent(XContentParser parser) {
        return PERSISTENT_TASKS_PARSER.apply(parser, null).build();
    }

    @SuppressWarnings("unchecked")
    public static <Params extends PersistentTaskParams> PersistentTasksCustomMetadata.PersistentTask<Params> getTaskWithId(
        ClusterState clusterState,
        String taskId
    ) {
        ClusterPersistentTasksCustomMetadata tasks = get(clusterState.metadata());
        if (tasks != null) {
            return (PersistentTasksCustomMetadata.PersistentTask<Params>) tasks.getTask(taskId);
        }
        return null;
    }

    @Override
    public long getLastAllocationId() {
        return lastAllocationId;
    }

    @Override
    public Map<String, PersistentTasksCustomMetadata.PersistentTask<?>> taskMap() {
        return this.tasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterPersistentTasksCustomMetadata that = (ClusterPersistentTasksCustomMetadata) o;
        return lastAllocationId == that.lastAllocationId && Objects.equals(tasks, that.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tasks, lastAllocationId);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        doWriteTo(out);
    }

    public static NamedDiff<Metadata.ClusterCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ClusterCustom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return doToXContentChunked();
    }

    @Override
    public Builder toBuilder() {
        return builder(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ClusterPersistentTasksCustomMetadata tasks) {
        return new Builder(tasks);
    }

    public static class Builder extends PersistentTasks.Builder<Builder> {

        protected Builder() {
            super();
        }

        protected Builder(PersistentTasks tasksInProgress) {
            super(tasksInProgress);
        }

        @Override
        public ClusterPersistentTasksCustomMetadata build() {
            return new ClusterPersistentTasksCustomMetadata(getLastAllocationId(), Collections.unmodifiableMap(getCurrentTasks()));
        }

        @Override
        protected ClusterState doBuildAndUpdate(ClusterState currentState, ProjectId projectId) {
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentState.metadata()).putCustom(ClusterPersistentTasksCustomMetadata.TYPE, build()))
                .build();
        }
    }

}
