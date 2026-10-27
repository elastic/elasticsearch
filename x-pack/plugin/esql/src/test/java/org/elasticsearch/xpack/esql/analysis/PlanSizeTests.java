/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PlanSizeTests extends ESTestCase {

    /** {@code depth} FORKs, each with two branches over the previous one by reference: 2^depth paths over 3 * depth + 1 nodes. */
    private static LogicalPlan sharedInputForks(int depth) {
        LogicalPlan plan = emptySource();
        for (int i = 0; i < depth; i++) {
            plan = new Fork(EMPTY, List.of(new Limit(EMPTY, null, plan), new Limit(EMPTY, null, plan)), List.of());
        }
        return plan;
    }

    private static LogicalPlan limitChain(int limits) {
        LogicalPlan plan = emptySource();
        for (int i = 0; i < limits; i++) {
            plan = new Limit(EMPTY, null, plan);
        }
        return plan;
    }

    public void testCountNodesCountsEveryNode() {
        PlanSize planSize = new PlanSize(0, Integer.MAX_VALUE);
        assertThat(planSize.countNodes(emptySource()), equalTo(1));
        assertThat(planSize.countNodes(limitChain(10)), equalTo(11));
        assertThat(planSize.countNodes(sharedInputForks(1)), equalTo(5));
    }

    /**
     * A shared input is counted once per parent, as analysis will copy it, but walked once: a 60-deep DAG has 2^60 paths and is counted
     * in a few hundred steps, saturating instead of overflowing.
     */
    public void testCountNodesWalksASharedInputOnce() {
        PlanSize planSize = new PlanSize(0, Integer.MAX_VALUE);
        assertThat(planSize.countNodes(sharedInputForks(20)), equalTo((1 << 22) - 3));
        assertThat(planSize.countNodes(sharedInputForks(60)), equalTo(Integer.MAX_VALUE));
    }

    /** The plans the parser checks in turn each hold the ones before, so the check of each one reads the earlier sizes. */
    public void testCheckFailsPastTheMaximumWithoutAddingToTheTotal() {
        PlanSize planSize = new PlanSize(0, 100);
        LogicalPlan plan = emptySource();
        for (int depth = 1; depth < 5; depth++) {
            plan = new Fork(EMPTY, List.of(new Limit(EMPTY, null, plan), new Limit(EMPTY, null, plan)), List.of());
            planSize.check(plan);
            assertThat(planSize.size(), equalTo(0));
        }
        LogicalPlan tooDeep = new Fork(EMPTY, List.of(new Limit(EMPTY, null, plan), new Limit(EMPTY, null, plan)), List.of());
        var e = expectThrows(VerificationException.class, () -> planSize.check(tooDeep));
        assertThat(e.getMessage(), containsString("maximum of [100] plan nodes"));
        assertThat(e.getMessage(), containsString("Found at least [125] plan nodes"));
        assertThat(planSize.size(), equalTo(0));
    }

    public void testAddFailsPastTheMaximumWithoutOverflowing() {
        PlanSize planSize = new PlanSize(490, 500);
        planSize.add(limitChain(9));
        assertThat(planSize.size(), equalTo(500));
        var e = expectThrows(VerificationException.class, () -> planSize.add(Integer.MAX_VALUE));
        assertThat(e.getMessage(), containsString("Found at least [" + Integer.MAX_VALUE + "] plan nodes"));
    }
}
