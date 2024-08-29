/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;

record RemoteClusterPlan(PhysicalPlan plan, String[] targetIndices, OriginalIndices originalIndices) {
    static RemoteClusterPlan from(PlanStreamInput planIn) throws IOException {
        var plan = planIn.readPhysicalPlanNode();
        var targetIndices = planIn.readStringArray();
        final OriginalIndices originalIndices;
        if (planIn.getTransportVersion().onOrAfter(TransportVersions.ESQL_ORIGINAL_INDICES)) {
            originalIndices = OriginalIndices.readOriginalIndices(planIn);
        } else {
            originalIndices = new OriginalIndices(planIn.readStringArray(), IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        }
        return new RemoteClusterPlan(plan, targetIndices, originalIndices);
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        out.writePhysicalPlanNode(plan);
        out.writeStringArray(targetIndices);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ORIGINAL_INDICES)) {
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        } else {
            out.writeStringArray(originalIndices.indices());
        }
    }
}
