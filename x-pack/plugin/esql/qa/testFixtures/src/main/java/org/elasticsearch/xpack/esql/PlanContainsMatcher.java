/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

/**
 * This validates if the actual plan contains the expected plan anywhere within the tree
 */
public class PlanContainsMatcher extends TypeSafeMatcher<LogicalPlan> {
    private final LogicalPlan expected;

    public static PlanContainsMatcher containsPlan(PlanBuilder expected) {
        return new PlanContainsMatcher(expected.build());
    }

    public static PlanContainsMatcher containsPlan(LogicalPlan expected) {
        return new PlanContainsMatcher(expected);
    }

    PlanContainsMatcher(LogicalPlan expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(LogicalPlan actual) {
        return matches(expected, actual);
    }

    @Override
    public void describeTo(Description description) {

    }

    private static List<LogicalPlan> findMatchingChildren(LogicalPlan expected, LogicalPlan actual) {
        return actual.collect(p -> p.getClass() == expected.getClass());
    }

    private static boolean matches(LogicalPlan expected, LogicalPlan initialActual) {
        List<LogicalPlan> candidates = findMatchingChildren(expected, initialActual);
        if (candidates.isEmpty()) {
            return false;
        }

        // TODO: Deal with multiple matching children case
        LogicalPlan actual = candidates.get(0);
        return PartialPlanMatcher.matches(expected, actual) == null;
    }
}
