/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.plan.physical.Unexecutable;
import org.elasticsearch.xpack.eql.plan.physical.UnplannedExec;
import org.elasticsearch.xpack.ql.common.Failure;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.ql.common.Failure.fail;

abstract class Verifier {

    static List<Failure> verifyMappingPlan(PhysicalPlan plan) {
        List<Failure> failures = new ArrayList<>();

        plan.forEachUp(p -> {
            if (p instanceof UnplannedExec) {
                failures.add(fail(p, "Unplanned item {}", ((UnplannedExec) p).plan().nodeName()));
            }
            p.forEachExpressionUp(e -> {
                if (e.childrenResolved() && e.resolved() == false) {
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
                failures.add(fail(p, "Unexecutable item {}", p.nodeName()));
            }
            p.forEachExpressionUp(e -> {
                if (e.childrenResolved() && e.resolved() == false) {
                    failures.add(fail(e, "Unresolved expression"));
                }
            });
        });

        return failures;
    }
}
