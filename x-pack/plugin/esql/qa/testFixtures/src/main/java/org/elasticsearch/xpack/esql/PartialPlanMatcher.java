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
        return matches(expected, actual) == null;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(expected.toString());
    }

    @Override
    protected void describeMismatchSafely(LogicalPlan item, Description mismatchDescription) {
        super.describeMismatchSafely(item, mismatchDescription);
        mismatchDescription.appendText(System.lineSeparator()).appendText(matches(expected, item));
    }

    static String matches(LogicalPlan expected, LogicalPlan actual) {
        if (expected.getClass() != actual.getClass()) {
            return "Mismatch in plan types: Expected [" + expected.getClass() + "], found [" + actual.getClass() + "]";
        }

        List<Object> expectedProperties = expected.nodeProperties();
        List<Object> actualProperties = actual.nodeProperties();

        for (int i = 0; i < expectedProperties.size(); i++) {
            Object expectedProperty = expectedProperties.get(i);
            Object actualProperty = actualProperties.get(i);

            // Only check equality if expected is not null. This way we can be lenient by simply
            // leaving out properties.
            if (expectedProperty == null || expectedProperty instanceof PlanBuilder.Ignore) {
                continue;
            }

            if (actualProperty == null) {
                return "Expected [" + expectedProperty.getClass() + "], found null.";
            }

            if (expectedProperty instanceof LogicalPlan l) {
                if (actualProperty instanceof LogicalPlan rightPlan) {
                    String subMatch = matches(l, rightPlan);
                    if (subMatch != null) {
                        return subMatch;
                    }
                } else {}
            } else if (expectedProperty instanceof Expression e) {
                if (actualProperty instanceof Expression rightExpr) {
                    if (e.semanticEquals(rightExpr) == false) {
                        return "Mismatch in expressions: Expected [" + e + "], found [" + rightExpr + "]";
                    }
                } else {
                    return "Mismatch in types: Expected [" + expectedProperty.getClass() + "], found [" + actualProperty.getClass() + "]";
                }
            } else if (expectedProperty.equals(actualProperty) == false) {
                return "Mismatch in properties: Expected [" + expectedProperty + "], found [" + actualProperty + "]";
            }
        }

        return null;
    }
}
