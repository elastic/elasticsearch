/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record PlanProfile(String description, String clusterName, String nodeName, String planTree, PlanTimeProfile planTimeProfile)
    implements
        Writeable,
        ToXContentObject {

    private static final TransportVersion PLAN_PROFILE_VERSION = TransportVersion.fromName("plan_profile_version");

    public static PlanProfile readFrom(StreamInput in) throws IOException {
        String description = in.readString();
        String clusterName = in.readString();
        String nodeName = in.readString();
        String planTree = in.readString();
        PlanTimeProfile profile = null;
        if (in.getTransportVersion().supports(PLAN_PROFILE_VERSION)) {
            profile = in.readOptionalWriteable(PlanTimeProfile::new);
        }

        return new PlanProfile(description, clusterName, nodeName, planTree, profile);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        out.writeString(clusterName);
        out.writeString(nodeName);
        out.writeString(planTree);
        if (out.getTransportVersion().supports(PLAN_PROFILE_VERSION)) {
            out.writeOptionalWriteable(planTimeProfile);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("description", description);
        builder.field("cluster_name", clusterName);
        builder.field("node_name", nodeName);
        builder.field("plan", planTree);
        if (planTimeProfile != null) {
            planTimeProfile.toXContent(builder, params);
        }

        return builder.endObject();
    }
}
