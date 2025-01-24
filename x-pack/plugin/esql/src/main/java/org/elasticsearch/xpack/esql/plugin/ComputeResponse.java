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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * The compute result of {@link DataNodeRequest} or {@link ClusterComputeRequest}
 */
final class ComputeResponse extends TransportResponse {
    private final List<DriverProfile> profiles;

    // for use with ClusterComputeRequests (cross-cluster searches)
    private final TimeValue took;  // overall took time for a specific cluster in a cross-cluster search
    public final int totalShards;
    public final int successfulShards;
    public final int skippedShards;
    public final int failedShards;

    ComputeResponse(List<DriverProfile> profiles) {
        this(profiles, null, null, null, null, null);
    }

    ComputeResponse(
        List<DriverProfile> profiles,
        TimeValue took,
        Integer totalShards,
        Integer successfulShards,
        Integer skippedShards,
        Integer failedShards
    ) {
        this.profiles = profiles;
        this.took = took;
        this.totalShards = totalShards == null ? 0 : totalShards.intValue();
        this.successfulShards = successfulShards == null ? 0 : successfulShards.intValue();
        this.skippedShards = skippedShards == null ? 0 : skippedShards.intValue();
        this.failedShards = failedShards == null ? 0 : failedShards.intValue();
    }

    ComputeResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            if (in.readBoolean()) {
                profiles = in.readCollectionAsImmutableList(DriverProfile::new);
            } else {
                profiles = null;
            }
        } else {
            profiles = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            this.took = in.readOptionalTimeValue();
            this.totalShards = in.readVInt();
            this.successfulShards = in.readVInt();
            this.skippedShards = in.readVInt();
            this.failedShards = in.readVInt();
        } else {
            this.took = new TimeValue(0L);
            this.totalShards = 0;
            this.successfulShards = 0;
            this.skippedShards = 0;
            this.failedShards = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            if (profiles == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeCollection(profiles);
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalTimeValue(took);
            out.writeVInt(totalShards);
            out.writeVInt(successfulShards);
            out.writeVInt(skippedShards);
            out.writeVInt(failedShards);
        }
    }

    public List<DriverProfile> getProfiles() {
        return profiles;
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
}
