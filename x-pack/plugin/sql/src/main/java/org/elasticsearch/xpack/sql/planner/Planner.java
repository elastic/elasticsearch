/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Verifier.Failure;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class Planner {

    private final Mapper mapper = new Mapper();
    private final QueryFolder folder = new QueryFolder();

    public PhysicalPlan plan(LogicalPlan plan) {
        return plan(plan, true);
    }

    public PhysicalPlan plan(LogicalPlan plan, boolean verify) {
        return foldPlan(mapPlan(plan, verify), verify);
    }

    // first, map the logical plan
    public PhysicalPlan mapPlan(LogicalPlan plan, boolean verify) {
        return verify ? verifyMappingPlan(mapper.map(plan)) : mapper.map(plan);
    }

    // second, pack it up
    public PhysicalPlan foldPlan(PhysicalPlan mapped, boolean verify) {
        return verify ? verifyExecutingPlan(folder.fold(mapped)) : folder.fold(mapped);
    }

    // verify the mapped plan
    public PhysicalPlan verifyMappingPlan(PhysicalPlan plan) {
        List<Failure> failures = Verifier.verifyMappingPlan(plan);
        if (!failures.isEmpty()) {
            throw new PlanningException(failures);
        }
        return plan;
    }

    public Map<Node<?>, String> verifyMappingPlanFailures(PhysicalPlan plan) {
        List<Failure> failures = Verifier.verifyMappingPlan(plan);
        return failures.stream().collect(toMap(Failure::source, Failure::message));
    }

    public PhysicalPlan verifyExecutingPlan(PhysicalPlan plan) {
        List<Failure> failures = Verifier.verifyExecutingPlan(plan);
        if (!failures.isEmpty()) {
            throw new PlanningException(failures);
        }
        return plan;
    }

    public Map<Node<?>, String> verifyExecutingPlanFailures(PhysicalPlan plan) {
        List<Failure> failures = Verifier.verifyExecutingPlan(plan);
        return failures.stream().collect(toMap(Failure::source, Failure::message));
    }
}
