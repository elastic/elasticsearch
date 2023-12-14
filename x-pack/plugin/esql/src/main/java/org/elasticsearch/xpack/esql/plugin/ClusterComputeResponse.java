/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.DriverProfile;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

final class ClusterComputeResponse extends ActionResponse {
    private final Collection<DriverProfile> profiles;

    ClusterComputeResponse(List<DriverProfile> profiles) {
        this.profiles = profiles;
    }

    ClusterComputeResponse(StreamInput in) throws IOException {
        super(in);
        this.profiles = in.readCollectionAsImmutableList(DriverProfile::new);
    }

    Collection<DriverProfile> getProfiles() {
        return profiles;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(profiles);
    }
}
