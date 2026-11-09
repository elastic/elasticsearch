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
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;

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

    /** Also guards the sink creation by the data node request; see {@link ExchangeService#SINK_CREATED_BY_DATA_REQUEST}. */
    private static final TransportVersion ESQL_DATA_NODE_PAGES_SENT = TransportVersion.fromName("esql_data_node_exchange_protocol");
    private final DriverCompletionInfo completionInfo;
    private final Map<ShardId, Exception> shardLevelFailures;
    /** Pages the data node pushed through its exchange sink, or -1 when unknown (older nodes); see {@link #pagesSent()}. */
    private final long pagesSent;

    DataNodeComputeResponse(DriverCompletionInfo completionInfo, Map<ShardId, Exception> shardLevelFailures) {
        this(completionInfo, shardLevelFailures, -1);
    }

    DataNodeComputeResponse(DriverCompletionInfo completionInfo, Map<ShardId, Exception> shardLevelFailures, long pagesSent) {
        this.completionInfo = completionInfo;
        this.shardLevelFailures = shardLevelFailures;
        this.pagesSent = pagesSent;
    }

    DataNodeComputeResponse(StreamInput in, ThreadContext threadContext) throws IOException {
        if (supportsCompletionInfo(in.getTransportVersion())) {
            this.completionInfo = DriverCompletionInfo.readFrom(in, threadContext);
            this.shardLevelFailures = in.readMap(ShardId::new, StreamInput::readException);
            this.pagesSent = in.getTransportVersion().supports(ESQL_DATA_NODE_PAGES_SENT) ? in.readZLong() : -1;
            return;
        }
        if (DataNodeComputeHandler.supportShardLevelRetryFailure(in.getTransportVersion())) {
            this.completionInfo = new DriverCompletionInfo(
                0,
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
            this.pagesSent = -1;
            return;
        }
        this.completionInfo = new ComputeResponse(in, threadContext).getCompletionInfo();
        this.shardLevelFailures = Map.of();
        this.pagesSent = -1;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportsCompletionInfo(out.getTransportVersion())) {
            completionInfo.writeTo(out);
            out.writeMap(shardLevelFailures, (o, v) -> v.writeTo(o), StreamOutput::writeException);
            if (out.getTransportVersion().supports(ESQL_DATA_NODE_PAGES_SENT)) {
                out.writeZLong(pagesSent);
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

    /**
     * The number of pages the data node pushed through the exchange sink of this request, which the coordinator uses to
     * finish its remote sink as soon as it has received them all instead of asking the sink once more. {@code -1} when the
     * data node did not report it.
     */
    long pagesSent() {
        return pagesSent;
    }
}
