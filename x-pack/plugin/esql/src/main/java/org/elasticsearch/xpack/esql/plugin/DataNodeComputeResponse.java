/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.esql.fetch.lifetime.OpenContextInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The compute result of {@link DataNodeRequest}
 */
final class DataNodeComputeResponse extends TransportResponse {

    private static final TransportVersion ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED = TransportVersion.fromName(
        "esql_documents_found_and_values_loaded"
    );

    private final DriverCompletionInfo completionInfo;
    private final Map<ShardId, Exception> shardLevelFailures;
    private final List<OpenContextInfo> openContexts;

    DataNodeComputeResponse(DriverCompletionInfo completionInfo, Map<ShardId, Exception> shardLevelFailures) {
        this(completionInfo, shardLevelFailures, List.of());
    }

    /**
     * @param openContexts the fetch contexts the data node keeps open after this response, for the coordinator to free
     */
    DataNodeComputeResponse(
        DriverCompletionInfo completionInfo,
        Map<ShardId, Exception> shardLevelFailures,
        List<OpenContextInfo> openContexts
    ) {
        this.completionInfo = completionInfo;
        this.shardLevelFailures = shardLevelFailures;
        this.openContexts = openContexts;
    }

    DataNodeComputeResponse(StreamInput in, ThreadContext threadContext) throws IOException {
        if (supportsCompletionInfo(in.getTransportVersion())) {
            this.completionInfo = DriverCompletionInfo.readFrom(in, threadContext);
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
            this.openContexts = in.getTransportVersion().supports(DataNodeRequest.ESQL_FETCH_CONTEXTS)
                ? in.readCollectionAsImmutableList(OpenContextInfo::new)
                : List.of();
            return;
        }
        this.openContexts = List.of();
        if (DataNodeComputeHandler.supportShardLevelRetryFailure(in.getTransportVersion())) {
            this.completionInfo = new DriverCompletionInfo(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                in.readCollectionAsImmutableList(DriverProfile::readFrom),
                List.of(),
                java.util.Map.of(),
                false,
                false,
                HeaderWarning.readWarningsFromThreadContext(threadContext)
            );
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
            return;
        }
        this.completionInfo = new ComputeResponse(in, threadContext).getCompletionInfo();
        this.shardLevelFailures = Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (openContexts.isEmpty() == false && out.getTransportVersion().supports(DataNodeRequest.ESQL_FETCH_CONTEXTS) == false) {
            // only a coordinator that asked for fetch contexts gets them, and it can read them
            throw new IllegalStateException("can't send fetch contexts to a node on [" + out.getTransportVersion() + "]");
        }
        if (supportsCompletionInfo(out.getTransportVersion())) {
            completionInfo.writeTo(out);
            out.writeMap(shardLevelFailures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
            if (out.getTransportVersion().supports(DataNodeRequest.ESQL_FETCH_CONTEXTS)) {
                out.writeCollection(openContexts);
            }
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

    private static boolean supportsCompletionInfo(TransportVersion version) {
        return version.supports(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED);
    }

    public DriverCompletionInfo completionInfo() {
        return completionInfo;
    }

    Map<ShardId, Exception> shardLevelFailures() {
        return shardLevelFailures;
    }

    List<OpenContextInfo> openContexts() {
        return openContexts;
    }

    DataNodeComputeResponse withOpenContexts(List<OpenContextInfo> openContexts) {
        return new DataNodeComputeResponse(completionInfo, shardLevelFailures, openContexts);
    }
}
