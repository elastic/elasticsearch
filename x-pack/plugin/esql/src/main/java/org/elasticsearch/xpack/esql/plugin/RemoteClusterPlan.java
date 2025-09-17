/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

record RemoteClusterPlan(PhysicalPlan plan, String[] targetIndices, OriginalIndices originalIndices) {
    static RemoteClusterPlan from(PlanStreamInput planIn) throws IOException {
        var plan = planIn.readNamedWriteable(PhysicalPlan.class);
        var targetIndices = planIn.readStringArray();
        OriginalIndices originalIndices = OriginalIndices.readOriginalIndices(planIn);
        return new RemoteClusterPlan(plan, targetIndices, originalIndices);
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        out.writeNamedWriteable(plan);
        out.writeStringArray(targetIndices);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RemoteClusterPlan that = (RemoteClusterPlan) o;
        return Objects.equals(plan, that.plan)
            && Objects.deepEquals(targetIndices, that.targetIndices)
            && Objects.equals(originalIndices, that.originalIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan, Arrays.hashCode(targetIndices), originalIndices);
    }
}
