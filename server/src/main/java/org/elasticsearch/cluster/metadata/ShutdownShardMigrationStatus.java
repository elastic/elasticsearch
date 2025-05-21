/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.endObject;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.startObject;

public class ShutdownShardMigrationStatus implements Writeable, ChunkedToXContentObject {

    public static final String NODE_ALLOCATION_DECISION_KEY = "node_allocation_decision";

    private final SingleNodeShutdownMetadata.Status status;
    private final long startedShards;
    private final long relocatingShards;
    private final long initializingShards;
    private final long shardsRemaining;
    @Nullable
    private final String explanation;
    @Nullable
    private final ShardAllocationDecision allocationDecision;

    public ShutdownShardMigrationStatus(
        SingleNodeShutdownMetadata.Status status,
        long shardsRemaining,
        @Nullable String explanation,
        @Nullable ShardAllocationDecision allocationDecision
    ) {
        this(status, -1, -1, -1, shardsRemaining, explanation, allocationDecision);
    }

    public ShutdownShardMigrationStatus(
        SingleNodeShutdownMetadata.Status status,
        long startedShards,
        long relocatingShards,
        long initializingShards
    ) {
        this(
            status,
            startedShards,
            relocatingShards,
            initializingShards,
            startedShards + relocatingShards + initializingShards,
            null,
            null
        );
    }

    public ShutdownShardMigrationStatus(
        SingleNodeShutdownMetadata.Status status,
        long startedShards,
        long relocatingShards,
        long initializingShards,
        @Nullable String explanation
    ) {
        this(
            status,
            startedShards,
            relocatingShards,
            initializingShards,
            startedShards + relocatingShards + initializingShards,
            explanation,
            null
        );
    }

    public ShutdownShardMigrationStatus(
        SingleNodeShutdownMetadata.Status status,
        long startedShards,
        long relocatingShards,
        long initializingShards,
        @Nullable String explanation,
        @Nullable ShardAllocationDecision allocationDecision
    ) {
        this(
            status,
            startedShards,
            relocatingShards,
            initializingShards,
            startedShards + relocatingShards + initializingShards,
            explanation,
            allocationDecision
        );
    }

    private ShutdownShardMigrationStatus(
        SingleNodeShutdownMetadata.Status status,
        long startedShards,
        long relocatingShards,
        long initializingShards,
        long shardsRemaining,
        @Nullable String explanation,
        @Nullable ShardAllocationDecision allocationDecision
    ) {
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.startedShards = startedShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.shardsRemaining = shardsRemaining;
        this.explanation = explanation;
        this.allocationDecision = allocationDecision;
    }

    public ShutdownShardMigrationStatus(StreamInput in) throws IOException {
        this.status = in.readEnum(SingleNodeShutdownMetadata.Status.class);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.startedShards = in.readZLong();
            this.relocatingShards = in.readZLong();
            this.initializingShards = in.readZLong();
            this.shardsRemaining = in.readZLong();
        } else {
            this.startedShards = -1;
            this.relocatingShards = -1;
            this.initializingShards = -1;
            this.shardsRemaining = in.readLong();
        }
        this.explanation = in.readOptionalString();
        this.allocationDecision = in.readOptionalWriteable(ShardAllocationDecision::new);
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

    public ShardAllocationDecision getAllocationDecision() {
        return allocationDecision;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            startObject(),
            chunk((builder, p) -> buildHeader(builder)),
            Objects.nonNull(allocationDecision)
                ? ChunkedToXContentHelper.object(NODE_ALLOCATION_DECISION_KEY, allocationDecision.toXContentChunked(params))
                : Collections.emptyIterator(),
            endObject()
        );
    }

    private XContentBuilder buildHeader(XContentBuilder builder) throws IOException {
        builder.field("status", status);
        if (startedShards != -1) {
            builder.field("started_shards", startedShards);
            builder.field("relocating_shards", relocatingShards);
            builder.field("initializing_shards", initializingShards);
        }
        builder.field("shard_migrations_remaining", shardsRemaining);
        if (Objects.nonNull(explanation)) {
            builder.field("explanation", explanation);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeZLong(startedShards);
            out.writeZLong(relocatingShards);
            out.writeZLong(initializingShards);
            out.writeZLong(shardsRemaining);
        } else {
            out.writeLong(shardsRemaining);
        }
        out.writeOptionalString(explanation);
        out.writeOptionalWriteable(allocationDecision);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShutdownShardMigrationStatus that = (ShutdownShardMigrationStatus) o;
        return startedShards == that.startedShards
            && relocatingShards == that.relocatingShards
            && initializingShards == that.initializingShards
            && shardsRemaining == that.shardsRemaining
            && status == that.status
            && Objects.equals(explanation, that.explanation)
            && Objects.equals(allocationDecision, that.allocationDecision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, startedShards, relocatingShards, initializingShards, shardsRemaining, explanation, allocationDecision);
    }

    @Override
    public String toString() {
        return Strings.toString((b, p) -> buildHeader(b), false, false);
    }
}
