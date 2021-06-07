/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
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
        Holder<Boolean> hasLimit = new Holder<>(Boolean.FALSE);
        Holder<List<Order>> orderBy = new Holder<>();
        plan.forEachUp(p -> {
            if (hasLimit.get() == false && p instanceof LimitExec) {
                hasLimit.set(Boolean.TRUE);
                return;
            }
            if (p instanceof OrderExec) {
                if (hasLimit.get() && orderBy.get() != null && ((OrderExec) p).order().equals(orderBy.get()) == false) {
                    failures.add(fail(p, "Cannot use ORDER BY on top of a subquery with ORDER BY and LIMIT"));
                } else {
                    orderBy.set(((OrderExec) p).order());
                }
            }
        });
    }
}
