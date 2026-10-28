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
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

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
        PlanSize planSize = PlanSize.unlimited();
        assertThat(planSize.countNodes(emptySource()), equalTo(1));
        assertThat(planSize.countNodes(limitChain(10)), equalTo(11));
        assertThat(planSize.countNodes(sharedInputForks(1)), equalTo(5));
    }

    /**
     * A shared input is counted once per parent, as analysis will copy it, but walked once: a 60-deep DAG has 2^60 paths and is counted
     * in a few hundred steps, saturating instead of overflowing.
     */
    public void testCountNodesWalksASharedInputOnce() {
        PlanSize planSize = PlanSize.unlimited();
        assertThat(planSize.countNodes(sharedInputForks(20)), equalTo((1 << 22) - 3));
        assertThat(planSize.countNodes(sharedInputForks(60)), equalTo(Integer.MAX_VALUE));
    }

    /**
     * The width of a plan is its leaf branches counted once per path, as {@link UnionAll#unionStats} counts them on the optimized plan:
     * a FORK over a shared input repeats the input's leaves per branch, so FORKs stacked through subqueries double them at every level.
     */
    public void testCheckPlanWidthCountsLeavesOncePerPath() {
        PlanSize planSize = PlanSize.unlimited();
        planSize.checkPlanWidth(limitChain(10), 1, "test");
        planSize.checkPlanWidth(sharedInputForks(1), 2, "test");
        planSize.checkPlanWidth(sharedInputForks(4), 16, "test");
        expectThrows(
            VerificationException.class,
            containsString("query resolved to 16 branches in total, exceeding the limit of 15 set by the test"),
            () -> planSize.checkPlanWidth(sharedInputForks(4), 15, "test")
        );
    }

    /**
     * The plans the parser checks in turn each hold the ones checked before, so each check reads the earlier stats: a 60-deep DAG
     * has 2^60 paths and is checked in a few hundred steps, its leaves saturating instead of overflowing.
     */
    public void testCheckPlanWidthWalksASharedInputOnce() {
        PlanSize planSize = PlanSize.unlimited();
        LogicalPlan plan = emptySource();
        for (int depth = 1; depth <= 60; depth++) {
            plan = new Fork(EMPTY, List.of(new Limit(EMPTY, null, plan), new Limit(EMPTY, null, plan)), List.of());
            planSize.checkPlanWidth(plan, Integer.MAX_VALUE, "test");
        }
        LogicalPlan deepest = plan;
        expectThrows(
            VerificationException.class,
            containsString("query resolved to " + Integer.MAX_VALUE + " branches in total"),
            () -> planSize.checkPlanWidth(deepest, Integer.MAX_VALUE - 1, "test")
        );
    }

    public void testAddFailsPastTheMaximumWithoutOverflowing() {
        PlanSize planSize = new PlanSize(490, 500);
        planSize.add(limitChain(9));
        assertThat(planSize.size(), equalTo(500));
        expectThrows(
            VerificationException.class,
            containsString("Found at least [" + Integer.MAX_VALUE + "] plan nodes"),
            () -> planSize.add(Integer.MAX_VALUE)
        );
    }

    public void testErrorNamesTheSourceOfTheMaximum() {
        PlanSize planSize = new PlanSize(limitChain(10), 20, "[max_command_count] query pragma");
        expectThrows(
            VerificationException.class,
            containsString("Found at least [22] plan nodes. The maximum is set by the [max_command_count] query pragma."),
            () -> planSize.add(limitChain(10))
        );
        expectThrows(
            VerificationException.class,
            containsString("The maximum is set by the [esql.query.max_command_count] cluster setting."),
            () -> new PlanSize(limitChain(10), 20, PlanSize.CLUSTER_SETTING_SOURCE).add(limitChain(10))
        );
    }
}
