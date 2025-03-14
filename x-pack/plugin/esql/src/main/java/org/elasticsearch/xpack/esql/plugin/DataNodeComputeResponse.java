/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The compute result of {@link DataNodeRequest}
 */
final class DataNodeComputeResponse extends TransportResponse {
    private final CollectedProfiles profiles;
    private final Map<ShardId, Exception> shardLevelFailures;

    DataNodeComputeResponse(CollectedProfiles profiles, Map<ShardId, Exception> shardLevelFailures) {
        this.profiles = profiles;
        this.shardLevelFailures = shardLevelFailures;
    }

    DataNodeComputeResponse(StreamInput in) throws IOException {
        if (DataNodeComputeHandler.supportShardLevelRetryFailure(in.getTransportVersion())) {
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PLANNER_PROFILE)) {
                this.profiles = new CollectedProfiles(in);
            } else {
                this.profiles = new CollectedProfiles(in.readCollectionAsImmutableList(DriverProfile::readFrom), List.of());
            }
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
        } else {
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PLANNER_PROFILE)) {
                this.profiles = new CollectedProfiles(in);
            } else {
                this.profiles = new CollectedProfiles(
                    Objects.requireNonNullElse(new ComputeResponse(in).getProfiles().getDriverProfiles(), List.of()),
                    List.of()
                );
            }
            this.shardLevelFailures = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (DataNodeComputeHandler.supportShardLevelRetryFailure(out.getTransportVersion())) {
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PLANNER_PROFILE)) {
                profiles.writeTo(out);
            } else {
                out.writeCollection(profiles.getDriverProfiles(), (o, v) -> v.writeTo(o));
            }
            out.writeMap(shardLevelFailures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
        } else {
            if (shardLevelFailures.isEmpty() == false) {
                throw new IllegalStateException("shard level failures are not supported in old versions");
            }
            new ComputeResponse(profiles).writeTo(out);
        }
    }

    CollectedProfiles profiles() {
        return profiles;
    }

    Map<ShardId, Exception> shardLevelFailures() {
        return shardLevelFailures;
    }
}
