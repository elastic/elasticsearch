/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.common.Failure;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.plan.physical.Unexecutable;
import org.elasticsearch.xpack.eql.plan.physical.UnplannedExec;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.eql.common.Failure.fail;

abstract class Verifier {

    static List<Failure> verifyMappingPlan(PhysicalPlan plan) {
        List<Failure> failures = new ArrayList<>();

        plan.forEachUp(p -> {
            if (p instanceof UnplannedExec) {
                failures.add(fail(p, "Unplanned item"));
            }
            p.forEachExpressionsUp(e -> {
                if (e.childrenResolved() && !e.resolved()) {
                    failures.add(fail(e, "Unresolved expression"));
                }
            });
        });
        return failures;
    }

    static List<Failure> verifyExecutingPlan(PhysicalPlan plan) {
        List<Failure> failures = new ArrayList<>();

        plan.forEachUp(p -> {
            if (p instanceof Unexecutable) {
                failures.add(fail(p, "Unexecutable item"));
            }
            p.forEachExpressionsUp(e -> {
                if (e.childrenResolved() && !e.resolved()) {
                    failures.add(fail(e, "Unresolved expression"));
                }
            });
        });

        return failures;
    }
}
