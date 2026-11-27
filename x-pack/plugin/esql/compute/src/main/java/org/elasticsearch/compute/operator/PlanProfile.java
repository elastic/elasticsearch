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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public record PlanProfile(
    String description,
    String clusterName,
    String nodeName,
    String planTree,
    String logicalPlanTree,
    PlanTimeProfile planTimeProfile,
    /**
     * Compact JSON object text for {@link #planTree}, embedded as an object in XContent.
     * Null when the peer node does not support {@link #JSON_PLANS_VERSION} or profiling did not capture it.
     */
    @Nullable String planJson,
    /** Compact JSON object text for {@link #logicalPlanTree}, or null when there is no logical tree. */
    @Nullable String logicalPlanJson,
    /** The phase of the query this plan ran in, when the query had several. See {@link ProfileLabels}. */
    @Nullable String phase,
    /** Why that phase ran. See {@link ProfileLabels}. */
    @Nullable String phaseKind
) implements Writeable, ToXContentObject {

    public static final ProfileSchema<PlanProfile> PROFILE_SCHEMA = ProfileSchema.of(PlanProfile.class, (builder, value, params) -> {
        builder.startObject();
        builder.field("description", value.description);
        builder.field("cluster_name", value.clusterName);
        builder.field("node_name", value.nodeName);
        ProfileLabels.write(builder, value.description, value.phase, value.phaseKind);
        builder.field("plan", value.planTree);
        if (value.planJson != null) {
            // Object, not a string of JSON. EXPLAIN's table cell stays a string because columns are scalar.
            builder.field("plan_json").rawValue(value.planJson);
        }
        if (value.logicalPlanTree != null) {
            builder.field("logical_plan", value.logicalPlanTree);
        }
        if (value.logicalPlanJson != null) {
            builder.field("logical_plan_json").rawValue(value.logicalPlanJson);
        }
        if (value.planTimeProfile != null) {
            PlanTimeProfile.PROFILE_SCHEMA.write(builder, value.planTimeProfile, params);
        }
        return builder.endObject();
    })
        .required("description", ProfileSchema.string())
        .required("cluster_name", ProfileSchema.string())
        .required("node_name", ProfileSchema.string())
        .optional("kind", ProfileSchema.string())
        .optional("branch", ProfileSchema.string())
        .optional("phase", ProfileSchema.string())
        .optional("phase_kind", ProfileSchema.string())
        .required("plan", ProfileSchema.string())
        .optional("plan_json", ProfileSchema.dynamicObject(Map.of()))
        .optional("logical_plan", ProfileSchema.string())
        .optional("logical_plan_json", ProfileSchema.dynamicObject(Map.of()))
        .optional("logical_optimization_nanos", ProfileSchema.integer())
        .optional("physical_optimization_nanos", ProfileSchema.integer())
        .optional("reduction_nanos", ProfileSchema.integer())
        .build();

    private static final TransportVersion PLAN_PROFILE_VERSION = TransportVersion.fromName("plan_profile_version");
    private static final TransportVersion LOGICAL_PLAN_VERSION = TransportVersion.fromName("esql_explain_only");
    /**
     * Optional {@code plan_json} / {@code logical_plan_json} strings, and operator {@code plan_node_id}.
     * Older nodes omit them. Ids are still local to the node that built the plan.
     */
    public static final TransportVersion JSON_PLANS_VERSION = TransportVersion.fromName("esql_json_query_plans");

    /** Plans captured before JSON fields existed. */
    public PlanProfile(
        String description,
        String clusterName,
        String nodeName,
        String planTree,
        String logicalPlanTree,
        PlanTimeProfile planTimeProfile
    ) {
        this(description, clusterName, nodeName, planTree, logicalPlanTree, planTimeProfile, null, null, null, null);
    }

    /** A plan of a query that ran in one phase. */
    public PlanProfile(
        String description,
        String clusterName,
        String nodeName,
        String planTree,
        String logicalPlanTree,
        PlanTimeProfile planTimeProfile,
        @Nullable String planJson,
        @Nullable String logicalPlanJson
    ) {
        this(description, clusterName, nodeName, planTree, logicalPlanTree, planTimeProfile, planJson, logicalPlanJson, null, null);
    }

    /** The same plan, labelled with the phase of the query it ran in. */
    public PlanProfile withPhase(@Nullable String phase, @Nullable String phaseKind) {
        return new PlanProfile(
            description,
            clusterName,
            nodeName,
            planTree,
            logicalPlanTree,
            planTimeProfile,
            planJson,
            logicalPlanJson,
            phase,
            phaseKind
        );
    }

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
        String planJson = null;
        String logicalPlanJson = null;
        String phase = null;
        String phaseKind = null;
        if (in.getTransportVersion().supports(JSON_PLANS_VERSION)) {
            planJson = in.readOptionalString();
            logicalPlanJson = in.readOptionalString();
            phase = in.readOptionalString();
            phaseKind = in.readOptionalString();
        }

        return new PlanProfile(
            description,
            clusterName,
            nodeName,
            planTree,
            logicalPlanTree,
            profile,
            planJson,
            logicalPlanJson,
            phase,
            phaseKind
        );
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
        if (out.getTransportVersion().supports(JSON_PLANS_VERSION)) {
            out.writeOptionalString(planJson);
            out.writeOptionalString(logicalPlanJson);
            out.writeOptionalString(phase);
            out.writeOptionalString(phaseKind);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return PROFILE_SCHEMA.write(builder, this, params);
    }
}
