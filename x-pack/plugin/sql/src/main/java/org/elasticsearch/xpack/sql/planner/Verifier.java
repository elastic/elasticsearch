/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.FilterExec;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PivotExec;
import org.elasticsearch.xpack.sql.plan.physical.UnaryExec;
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
            p.forEachExpressionUp(e -> {
                if (e.childrenResolved() && e.resolved() == false) {
                    failures.add(fail(e, "Unresolved expression"));
                }
            });
        });

        checkForNonCollapsableSubselects(plan, failures);

        return failures;
    }

    static List<Failure> verifyExecutingPlan(PhysicalPlan plan) {
        List<Failure> failures = new ArrayList<>();

        plan.forEachUp(p -> {
            if (p instanceof Unexecutable) {
                failures.add(fail(p, "Unexecutable item"));
            }
            p.forEachExpressionUp(e -> {
                if (e.childrenResolved() && e.resolved() == false) {
                    failures.add(fail(e, "Unresolved expression"));
                }
            });
        });

        return failures;
    }

    private static void checkForNonCollapsableSubselects(PhysicalPlan plan, List<Failure> failures) {
        Holder<LimitExec> limit = new Holder<>();
        Holder<UnaryExec> limitedExec = new Holder<>();

        plan.forEachUp(p -> {
            if (limit.get() == null && p instanceof LimitExec) {
                limit.set((LimitExec) p);
            } else if (limit.get() != null && limitedExec.get() == null) {
                if (p instanceof OrderExec || p instanceof FilterExec || p instanceof PivotExec || p instanceof AggregateExec) {
                    limitedExec.set((UnaryExec) p);
                }
            }
        });

        if (limitedExec.get() != null) {
            failures.add(
                fail(limit.get(), "LIMIT or TOP cannot be used in a subquery if outer query contains GROUP BY, ORDER BY, PIVOT or WHERE")
            );
        }
    }
}
