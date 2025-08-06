/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

/**
 * A matcher that compares a plan with a partial plan.
 */
public class PartialPlanMatcher extends TypeSafeMatcher<LogicalPlan> {
    private final LogicalPlan expected;

    public static PartialPlanMatcher matchesPartialPlan(PlanBuilder expected) {
        return new PartialPlanMatcher(expected.build());
    }

    public static PartialPlanMatcher matchesPartialPlan(LogicalPlan expected) {
        return new PartialPlanMatcher(expected);
    }

    PartialPlanMatcher(LogicalPlan expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(LogicalPlan actual) {
        return matches(expected, actual);
    }

    @Override
    public void describeTo(Description description) {

    }

    static boolean matches(LogicalPlan expected, LogicalPlan actual) {
        if (expected.getClass() != actual.getClass()) {
            assert false : "Mismatch in plan types: Expected [" + expected.getClass() + "], found [" + actual.getClass() + "]";
            return false;
        }

        List<Object> leftProperties = expected.nodeProperties();
        List<Object> rightProperties = actual.nodeProperties();

        for (int i = 0; i < leftProperties.size(); i++) {
            Object leftProperty = leftProperties.get(i);
            Object rightProperty = rightProperties.get(i);

            // Only check equality if left is not null. This way we can be lenient by simply
            // leaving out properties.
            if (leftProperty == null || leftProperty instanceof PlanBuilder.Ignore) {
                continue;
            }

            if (rightProperty == null) {
                assert false : "Expected [" + leftProperty.getClass() + "], found null.";
                return false;
            }

            if (leftProperty instanceof LogicalPlan l) {
                if (rightProperty instanceof LogicalPlan rightPlan) {
                    if (matches(l, rightPlan) == false) {
                        return false;
                    }
                } else {
                    assert false
                        : "Mismatch in types: Expected [" + leftProperty.getClass() + "], found [" + rightProperty.getClass() + "]";
                    return false;
                }
            } else if (leftProperty instanceof Expression e) {
                if (rightProperty instanceof Expression rightExpr) {
                    if (e.semanticEquals(rightExpr) == false) {
                        assert false : "Mismatch in expressions: Expected [" + e + "], found [" + rightExpr + "]";
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (leftProperty.equals(rightProperty) == false) {
                assert false : "Mismatch in properties: Expected [" + leftProperty + "], found [" + rightProperty + "]";
                return false;
            }
        }

        return true;
    }
}
