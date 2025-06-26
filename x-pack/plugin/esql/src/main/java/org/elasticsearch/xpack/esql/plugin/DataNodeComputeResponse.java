/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.TransportVersions.ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED;

/**
 * The compute result of {@link DataNodeRequest}
 */
final class DataNodeComputeResponse extends TransportResponse {
    private final DriverCompletionInfo completionInfo;
    private final Map<ShardId, Exception> shardLevelFailures;

    DataNodeComputeResponse(DriverCompletionInfo completionInfo, Map<ShardId, Exception> shardLevelFailures) {
        this.completionInfo = completionInfo;
        this.shardLevelFailures = shardLevelFailures;
    }

    DataNodeComputeResponse(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED)) {
            this.completionInfo = DriverCompletionInfo.readFrom(in);
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
            return;
        }
        if (DataNodeComputeHandler.supportShardLevelRetryFailure(in.getTransportVersion())) {
            this.completionInfo = new DriverCompletionInfo(0, 0, in.readCollectionAsImmutableList(DriverProfile::readFrom), List.of());
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
            return;
        }
        this.completionInfo = new ComputeResponse(in).getCompletionInfo();
        this.shardLevelFailures = Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED)) {
            completionInfo.writeTo(out);
            out.writeMap(shardLevelFailures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
            return;
        }
        if (DataNodeComputeHandler.supportShardLevelRetryFailure(out.getTransportVersion())) {
            out.writeCollection(completionInfo.driverProfiles());
            out.writeMap(shardLevelFailures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
            return;
        }
        if (shardLevelFailures.isEmpty() == false) {
            throw new IllegalStateException("shard level failures are not supported in old versions");
        }
        new ComputeResponse(completionInfo).writeTo(out);
    }

    public DriverCompletionInfo completionInfo() {
        return completionInfo;
    }

    Map<ShardId, Exception> shardLevelFailures() {
        return shardLevelFailures;
    }
}
