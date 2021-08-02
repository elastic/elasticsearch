/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.FilterExec;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PivotExec;
import org.elasticsearch.xpack.sql.plan.physical.Unexecutable;
import org.elasticsearch.xpack.sql.plan.physical.UnplannedExec;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
        // use a comparator to ensure deterministic order of the error messages
        Set<LimitExec> limits = new TreeSet<>(Comparator.comparing(l -> l.source().text()));

        plan.forEachDown(p -> {
            if (p instanceof OrderExec || p instanceof FilterExec || p instanceof PivotExec || p instanceof AggregateExec) {
                p.forEachDown(LimitExec.class, limits::add);
            }
        });

        for (LimitExec limit : limits) {
            failures.add(
                fail(limit, "LIMIT or TOP cannot be used in a subquery if outer query contains GROUP BY, WHERE, ORDER BY or PIVOT")
            );
        }
    }
}
