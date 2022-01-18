/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ShutdownShardMigrationStatus implements Writeable, ToXContentObject {
    private static final Version ALLOCATION_DECISION_ADDED_VERSION = Version.V_7_16_0;

    private final SingleNodeShutdownMetadata.Status status;
    private final long shardsRemaining;
    @Nullable
    private final String explanation;
    @Nullable
    private final ShardAllocationDecision allocationDecision;

    public ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status status, long shardsRemaining) {
        this(status, shardsRemaining, null, null);
    }

    public ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status status, long shardsRemaining, @Nullable String explanation) {
        this(status, shardsRemaining, explanation, null);
    }

    public ShutdownShardMigrationStatus(
        SingleNodeShutdownMetadata.Status status,
        long shardsRemaining,
        @Nullable String explanation,
        @Nullable ShardAllocationDecision allocationDecision
    ) {
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.shardsRemaining = shardsRemaining;
        this.explanation = explanation;
        this.allocationDecision = allocationDecision;
    }

    public ShutdownShardMigrationStatus(StreamInput in) throws IOException {
        this.status = in.readEnum(SingleNodeShutdownMetadata.Status.class);
        this.shardsRemaining = in.readLong();
        this.explanation = in.readOptionalString();
        if (in.getVersion().onOrAfter(ALLOCATION_DECISION_ADDED_VERSION)) {
            this.allocationDecision = in.readOptionalWriteable(ShardAllocationDecision::new);
        } else {
            this.allocationDecision = null;
        }
    }

    public long getShardsRemaining() {
        return shardsRemaining;
    }

    public String getExplanation() {
        return explanation;
    }

    public SingleNodeShutdownMetadata.Status getStatus() {
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.field("shard_migrations_remaining", shardsRemaining);
        if (Objects.nonNull(explanation)) {
            builder.field("explanation", explanation);
        }
        if (Objects.nonNull(allocationDecision)) {
            builder.startObject("node_allocation_decision");
            {
                allocationDecision.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeLong(shardsRemaining);
        out.writeOptionalString(explanation);
        if (out.getVersion().onOrAfter(ALLOCATION_DECISION_ADDED_VERSION)) {
            out.writeOptionalWriteable(allocationDecision);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof ShutdownShardMigrationStatus) == false) return false;
        ShutdownShardMigrationStatus that = (ShutdownShardMigrationStatus) o;
        return shardsRemaining == that.shardsRemaining
            && status == that.status
            && Objects.equals(explanation, that.explanation)
            && Objects.equals(allocationDecision, that.allocationDecision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, shardsRemaining, explanation, allocationDecision);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
