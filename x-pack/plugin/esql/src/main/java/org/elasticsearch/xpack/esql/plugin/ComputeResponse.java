/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * The compute result of {@link DataNodeRequest} or {@link ClusterComputeRequest}
 */
final class ComputeResponse extends TransportResponse {

    private static final TransportVersion ESQL_FAILURE_FROM_REMOTE = TransportVersion.fromName("esql_failure_from_remote");
    private static final TransportVersion ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED = TransportVersion.fromName(
        "esql_documents_found_and_values_loaded"
    );

    private final DriverCompletionInfo completionInfo;

    // for use with ClusterComputeRequests (cross-cluster searches)
    private final TimeValue took;  // overall took time for a specific cluster in a cross-cluster search
    public final int totalShards;
    public final int successfulShards;
    public final int skippedShards;
    public final int failedShards;
    public final List<ShardSearchFailure> failures;

    ComputeResponse(DriverCompletionInfo completionInfo) {
        this(completionInfo, null, null, null, null, null, List.of());
    }

    ComputeResponse(
        DriverCompletionInfo completionInfo,
        TimeValue took,
        Integer totalShards,
        Integer successfulShards,
        Integer skippedShards,
        Integer failedShards,
        List<ShardSearchFailure> failures
    ) {
        this.completionInfo = completionInfo;
        this.took = took;
        this.totalShards = totalShards == null ? 0 : totalShards.intValue();
        this.successfulShards = successfulShards == null ? 0 : successfulShards.intValue();
        this.skippedShards = skippedShards == null ? 0 : skippedShards.intValue();
        this.failedShards = failedShards == null ? 0 : failedShards.intValue();
        this.failures = failures;
    }

    ComputeResponse(StreamInput in) throws IOException {
        if (supportsCompletionInfo(in.getTransportVersion())) {
            completionInfo = DriverCompletionInfo.readFrom(in);
        } else {
            if (in.readBoolean()) {
                completionInfo = new DriverCompletionInfo(0, 0, in.readCollectionAsImmutableList(DriverProfile::readFrom), List.of());
            } else {
                completionInfo = DriverCompletionInfo.EMPTY;
            }
        }
        this.took = in.readOptionalTimeValue();
        this.totalShards = in.readVInt();
        this.successfulShards = in.readVInt();
        this.skippedShards = in.readVInt();
        this.failedShards = in.readVInt();
        if (in.getTransportVersion().supports(ESQL_FAILURE_FROM_REMOTE)) {
            this.failures = in.readCollectionAsImmutableList(ShardSearchFailure::readShardSearchFailure);
        } else {
            this.failures = List.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportsCompletionInfo(out.getTransportVersion())) {
            completionInfo.writeTo(out);
        } else {
            out.writeBoolean(true);
            out.writeCollection(completionInfo.driverProfiles());
        }
        out.writeOptionalTimeValue(took);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(skippedShards);
        out.writeVInt(failedShards);
        if (out.getTransportVersion().supports(ESQL_FAILURE_FROM_REMOTE)) {
            out.writeCollection(failures, (o, v) -> v.writeTo(o));
        }
    }

    private static boolean supportsCompletionInfo(TransportVersion version) {
        return version.supports(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED);
    }

    public DriverCompletionInfo getCompletionInfo() {
        return completionInfo;
    }

    public TimeValue getTook() {
        return took;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getSuccessfulShards() {
        return successfulShards;
    }

    public int getSkippedShards() {
        return skippedShards;
    }

    public int getFailedShards() {
        return failedShards;
    }

    public List<ShardSearchFailure> getFailures() {
        return failures;
    }
}
