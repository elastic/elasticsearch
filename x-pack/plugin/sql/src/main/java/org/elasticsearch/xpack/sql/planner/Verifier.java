/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PivotExec;
import org.elasticsearch.xpack.sql.plan.physical.Unexecutable;
import org.elasticsearch.xpack.sql.plan.physical.UnplannedExec;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.ql.common.Failure.fail;

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
        // verify Pivot
        checkInnerAggsPivot(plan, failures);

        return failures;
    }

    private static void checkInnerAggsPivot(PhysicalPlan plan, List<Failure> failures) {
        plan.forEachDown(p -> {
            p.pivot().aggregates().forEach(agg -> agg.forEachDown(e -> {
                if (e instanceof InnerAggregate) {
                    failures.add(fail(e, "Aggregation [{}] not supported (yet) by PIVOT", e.sourceText()));
                }
            }));
        }, PivotExec.class);
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
