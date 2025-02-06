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
    private final List<DriverProfile> profiles;
    private final Map<ShardId, Exception> shardLevelFailures;

    DataNodeComputeResponse(List<DriverProfile> profiles, Map<ShardId, Exception> shardLevelFailures) {
        this.profiles = profiles;
        this.shardLevelFailures = shardLevelFailures;
    }

    DataNodeComputeResponse(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_RETRY_ON_SHARD_LEVEL_FAILURE)) {
            this.profiles = in.readCollectionAsImmutableList(DriverProfile::new);
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
        } else {
            this.profiles = Objects.requireNonNullElse(new ComputeResponse(in).getProfiles(), List.of());
            this.shardLevelFailures = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_RETRY_ON_SHARD_LEVEL_FAILURE)) {
            out.writeCollection(profiles, (o, v) -> v.writeTo(o));
            out.writeMap(shardLevelFailures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
        } else {
            if (shardLevelFailures.isEmpty() == false) {
                throw new IllegalStateException("shard level failures are not supported in old versions");
            }
            new ComputeResponse(profiles).writeTo(out);
        }
    }

    List<DriverProfile> profiles() {
        return profiles;
    }

    Map<ShardId, Exception> shardLevelFailures() {
        return shardLevelFailures;
    }
}
