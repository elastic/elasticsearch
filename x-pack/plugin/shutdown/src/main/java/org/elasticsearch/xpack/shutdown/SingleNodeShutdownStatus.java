/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class SingleNodeShutdownStatus implements Writeable, ChunkedToXContentObject {

    private final SingleNodeShutdownMetadata metadata;
    private final ShutdownShardMigrationStatus shardMigrationStatus;
    private final ShutdownPersistentTasksStatus persistentTasksStatus;
    private final ShutdownPluginsStatus pluginsStatus;

    private static final ParseField STATUS = new ParseField("status");
    private static final ParseField SHARD_MIGRATION_FIELD = new ParseField("shard_migration");
    private static final ParseField PERSISTENT_TASKS_FIELD = new ParseField("persistent_tasks");
    private static final ParseField PLUGINS_STATUS = new ParseField("plugins");
    private static final ParseField TARGET_NODE_NAME_FIELD = new ParseField("target_node_name");

    public SingleNodeShutdownStatus(
        SingleNodeShutdownMetadata metadata,
        ShutdownShardMigrationStatus shardMigrationStatus,
        ShutdownPersistentTasksStatus persistentTasksStatus,
        ShutdownPluginsStatus pluginsStatus
    ) {
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.shardMigrationStatus = Objects.requireNonNull(shardMigrationStatus, "shard migration status must not be null");
        this.persistentTasksStatus = Objects.requireNonNull(persistentTasksStatus, "persistent task status must not be null");
        this.pluginsStatus = Objects.requireNonNull(pluginsStatus, "plugin status must not be null");
    }

    public SingleNodeShutdownStatus(StreamInput in) throws IOException {
        this.metadata = new SingleNodeShutdownMetadata(in);
        this.shardMigrationStatus = new ShutdownShardMigrationStatus(in);
        this.persistentTasksStatus = new ShutdownPersistentTasksStatus(in);
        this.pluginsStatus = new ShutdownPluginsStatus(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.metadata.writeTo(out);
        this.shardMigrationStatus.writeTo(out);
        this.persistentTasksStatus.writeTo(out);
        this.pluginsStatus.writeTo(out);
    }

    public SingleNodeShutdownMetadata.Status overallStatus() {
        return SingleNodeShutdownMetadata.Status.combine(
            migrationStatus().getStatus(),
            pluginsStatus().getStatus(),
            persistentTasksStatus().getStatus()
        );
    }

    public ShutdownShardMigrationStatus migrationStatus() {
        return this.shardMigrationStatus;
    }

    public ShutdownPersistentTasksStatus persistentTasksStatus() {
        return this.persistentTasksStatus;
    }

    public ShutdownPluginsStatus pluginsStatus() {
        return this.pluginsStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata, shardMigrationStatus, persistentTasksStatus, pluginsStatus);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SingleNodeShutdownStatus other = (SingleNodeShutdownStatus) obj;
        return metadata.equals(other.metadata)
            && shardMigrationStatus.equals(other.shardMigrationStatus)
            && persistentTasksStatus.equals(other.persistentTasksStatus)
            && pluginsStatus.equals(other.pluginsStatus);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).object(b -> {
            b.append((builder, p) -> {
                builder.field(SingleNodeShutdownMetadata.NODE_ID_FIELD.getPreferredName(), metadata.getNodeId());
                builder.field(SingleNodeShutdownMetadata.TYPE_FIELD.getPreferredName(), metadata.getType());
                builder.field(SingleNodeShutdownMetadata.REASON_FIELD.getPreferredName(), metadata.getReason());
                if (metadata.getAllocationDelay() != null) {
                    builder.field(
                        SingleNodeShutdownMetadata.ALLOCATION_DELAY_FIELD.getPreferredName(),
                        metadata.getAllocationDelay().getStringRep()
                    );
                }
                builder.timestampFieldsFromUnixEpochMillis(
                    SingleNodeShutdownMetadata.STARTED_AT_MILLIS_FIELD.getPreferredName(),
                    SingleNodeShutdownMetadata.STARTED_AT_READABLE_FIELD,
                    metadata.getStartedAtMillis()
                );
                builder.field(STATUS.getPreferredName(), overallStatus());
                return builder;
            });
            b.field(SHARD_MIGRATION_FIELD.getPreferredName(), shardMigrationStatus);
            b.append((builder, p) -> {
                builder.field(PERSISTENT_TASKS_FIELD.getPreferredName(), persistentTasksStatus);
                builder.field(PLUGINS_STATUS.getPreferredName(), pluginsStatus);
                if (metadata.getTargetNodeName() != null) {
                    builder.field(TARGET_NODE_NAME_FIELD.getPreferredName(), metadata.getTargetNodeName());
                }
                if (metadata.getGracePeriod() != null) {
                    builder.timestampField(
                        SingleNodeShutdownMetadata.GRACE_PERIOD_FIELD.getPreferredName(),
                        metadata.getGracePeriod().getStringRep()
                    );
                }
                return builder;
            });
        });
    }
}
