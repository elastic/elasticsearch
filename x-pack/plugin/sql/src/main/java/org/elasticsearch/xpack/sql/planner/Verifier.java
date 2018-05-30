/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.Unexecutable;
import org.elasticsearch.xpack.sql.plan.physical.UnplannedExec;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

abstract class Verifier {

    static class Failure {
        private final Node<?> source;
        private final String message;

        Failure(Node<?> source, String message) {
            this.source = source;
            this.message = message;
        }

        Node<?> source() {
            return source;
        }

        String message() {
            return message;
        }

        @Override
        public int hashCode() {
            return source.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Verifier.Failure other = (Verifier.Failure) obj;
            return Objects.equals(source, other.source);
        }
    }

    private static Failure fail(Node<?> source, String message) {
        return new Failure(source, message);
    }

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

            if (p instanceof AggregateExec) {
                forbidMultiFieldGroupBy((AggregateExec) p, failures);
            }
        });

        return failures;
    }

    private static void forbidMultiFieldGroupBy(AggregateExec a, List<Failure> failures) {
        if (a.groupings().size() > 1) {
            failures.add(fail(a.groupings().get(0), "Currently, only a single expression can be used with GROUP BY; please select one of "
                    + Expressions.names(a.groupings())));
        }
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
