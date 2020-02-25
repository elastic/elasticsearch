/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.List;

public class Planner {

    private final Mapper mapper = new Mapper();
    private final QueryFolder folder = new QueryFolder();

    public PhysicalPlan plan(LogicalPlan plan) {
        return foldPlan(mapPlan(plan));
    }

    PhysicalPlan mapPlan(LogicalPlan plan) {
        return verifyMappingPlan(mapper.map(plan));
    }

    PhysicalPlan foldPlan(PhysicalPlan mapped) {
        return verifyExecutingPlan(folder.fold(mapped));
    }

    PhysicalPlan verifyMappingPlan(PhysicalPlan plan) {
        List<Failure> failures = Verifier.verifyMappingPlan(plan);
        if (failures.isEmpty() == false) {
            throw new PlanningException(failures);
        }
        return plan;
    }

    PhysicalPlan verifyExecutingPlan(PhysicalPlan plan) {
        List<Failure> failures = Verifier.verifyExecutingPlan(plan);
        if (failures.isEmpty() == false) {
            throw new PlanningException(failures);
        }
        return plan;
    }
}
