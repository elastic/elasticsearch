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
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * The compute result of {@link DataNodeRequest} or {@link ClusterComputeRequest}
 */
final class ComputeResponse extends TransportResponse {
    private final List<DriverProfile> profiles;

    ComputeResponse(List<DriverProfile> profiles) {
        this.profiles = profiles;
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
    }

    public List<DriverProfile> getProfiles() {
        return profiles;
    }
}
