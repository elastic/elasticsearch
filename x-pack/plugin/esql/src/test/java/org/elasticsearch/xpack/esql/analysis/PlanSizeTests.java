/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PlanSizeTests extends ESTestCase {

    private static LogicalPlan row() {
        return new Row(EMPTY, List.of(new Alias(EMPTY, "x", new Literal(EMPTY, 1, DataType.INTEGER))));
    }

    /** A chain of {@code limits} LIMITs over a ROW, {@code limits + 1} nodes. */
    private static LogicalPlan limitChain(int limits) {
        LogicalPlan plan = row();
        for (int i = 0; i < limits; i++) {
            plan = new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), plan);
        }
        return plan;
    }

    /** {@code forks} nested FORKs whose branches share their input: {@code forks + 1} nodes, {@code 2^forks} paths. */
    private static LogicalPlan sharedInputForks(int forks) {
        LogicalPlan plan = row();
        for (int i = 0; i < forks; i++) {
            plan = new Fork(EMPTY, List.of(plan, plan), List.of());
        }
        return plan;
    }

    public void testOfCountsEveryNodeWithinTheMaximum() {
        LogicalPlan plan = limitChain(10);
        assertThat(PlanSize.of(plan, 11).size(), equalTo(11));
        assertThat(PlanSize.countNodes(plan), equalTo(11));
    }

    /** A parsed plan past the maximum is allowed until something is expanded into it. */
    public void testOfKeepsAPlanPastTheMaximumUntilSomethingIsAdded() {
        PlanSize planSize = PlanSize.of(limitChain(10), 10);
        assertThat(planSize.size(), equalTo(11));
        var e = expectThrows(VerificationException.class, () -> planSize.add(1));
        assertThat(e.getMessage(), containsString("ES|QL query exceeded the maximum of [10] plan nodes allowed"));
        assertThat(e.getMessage(), containsString("Found at least [12] plan nodes"));
    }

    /** One FORK over a query at the maximum is the most a parsed plan may hold; it is counted in full. */
    public void testOfCountsUpToOneForkOverAPlanAtTheMaximum() {
        LogicalPlan plan = limitChain(499);
        plan = new Fork(EMPTY, Collections.nCopies(QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE_MAX, plan), List.of());
        assertThat(PlanSize.of(plan, 500).size(), equalTo(500 * QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE_MAX + 1));
    }

    /** Counting a shared-input DAG once per path would take 2^60 steps; the count gives up as soon as the ceiling is passed. */
    public void testOfGivesUpOnASharedInputDag() {
        var e = expectThrows(VerificationException.class, () -> PlanSize.of(sharedInputForks(60), 500));
        assertThat(e.getMessage(), containsString("ES|QL query exceeded the maximum of [500] plan nodes allowed"));
        int ceiling = 500 * (QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE_MAX + 1);
        assertThat(e.getMessage(), containsString("Found at least [" + (ceiling + 1) + "] plan nodes"));
    }

    public void testAddNodesOfStopsAtTheRemainingBudget() {
        PlanSize planSize = new PlanSize(490, 500);
        planSize.addNodesOf(limitChain(4));
        assertThat(planSize.size(), equalTo(495));
        var e = expectThrows(VerificationException.class, () -> planSize.addNodesOf(sharedInputForks(60)));
        assertThat(e.getMessage(), containsString("Found at least [501] plan nodes"));
    }
}
