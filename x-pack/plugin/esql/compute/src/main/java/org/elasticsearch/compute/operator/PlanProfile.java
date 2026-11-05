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

public record PlanProfile(
    String description,
    String clusterName,
    String nodeName,
    String planTree,
    String logicalPlanTree,
    PlanTimeProfile planTimeProfile
) implements Writeable, ToXContentObject {

    public static final ProfileSchema<PlanProfile> PROFILE_SCHEMA = ProfileSchema.of(PlanProfile.class, (builder, value, params) -> {
        builder.startObject();
        builder.field("description", value.description);
        builder.field("cluster_name", value.clusterName);
        builder.field("node_name", value.nodeName);
        builder.field("plan", value.planTree);
        if (value.logicalPlanTree != null) {
            builder.field("logical_plan", value.logicalPlanTree);
        }
        if (value.planTimeProfile != null) {
            PlanTimeProfile.PROFILE_SCHEMA.write(builder, value.planTimeProfile, params);
        }
        return builder.endObject();
    })
        .required("description", ProfileSchema.string())
        .required("cluster_name", ProfileSchema.string())
        .required("node_name", ProfileSchema.string())
        .required("plan", ProfileSchema.string())
        .optional("logical_plan", ProfileSchema.string())
        .optional("logical_optimization_nanos", ProfileSchema.integer())
        .optional("physical_optimization_nanos", ProfileSchema.integer())
        .optional("reduction_nanos", ProfileSchema.integer())
        .build();

    private static final TransportVersion PLAN_PROFILE_VERSION = TransportVersion.fromName("plan_profile_version");
    private static final TransportVersion LOGICAL_PLAN_VERSION = TransportVersion.fromName("esql_explain_only");

    public static PlanProfile readFrom(StreamInput in) throws IOException {
        String description = in.readString();
        String clusterName = in.readString();
        String nodeName = in.readString();
        String planTree = in.readString();
        String logicalPlanTree = null;
        if (in.getTransportVersion().supports(LOGICAL_PLAN_VERSION)) {
            logicalPlanTree = in.readOptionalString();
        }
        PlanTimeProfile profile = null;
        if (in.getTransportVersion().supports(PLAN_PROFILE_VERSION)) {
            profile = in.readOptionalWriteable(PlanTimeProfile::new);
        }

        return new PlanProfile(description, clusterName, nodeName, planTree, logicalPlanTree, profile);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        out.writeString(clusterName);
        out.writeString(nodeName);
        out.writeString(planTree);
        if (out.getTransportVersion().supports(LOGICAL_PLAN_VERSION)) {
            out.writeOptionalString(logicalPlanTree);
        }
        if (out.getTransportVersion().supports(PLAN_PROFILE_VERSION)) {
            out.writeOptionalWriteable(planTimeProfile);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return PROFILE_SCHEMA.write(builder, this, params);
    }
}
