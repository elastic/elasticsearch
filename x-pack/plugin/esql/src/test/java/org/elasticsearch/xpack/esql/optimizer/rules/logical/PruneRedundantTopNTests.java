/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PruneRedundantTopNTests extends AbstractLogicalPlanOptimizerTests {
    private static final List<Order> ORDER = List.of(new Order(EMPTY, getFieldAttribute("a"), Order.OrderDirection.ASC, null));

    public PruneRedundantTopNTests(VersionMode versionMode) {
        super(versionMode);
    }

    private LogicalPlan prune(TopN topN) {
        return new PruneRedundantTopN().apply(topN, logicalOptimizerCtx);
    }

    /**
     * A non-local TopN is a pipeline breaker, so it must survive merging regardless of whether it is the parent or the child.
     * This is hard to reproduce through real query paths, hence the hand-built plans.
     */
    public void testNonLocalTopNSurvivesMergeWithLocalTopN() {
        var source = emptySource();
        for (boolean parentLocal : new boolean[] { true, false }) {
            var childTopN = new TopN(EMPTY, source, ORDER, L(100), parentLocal == false);
            var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(10), parentLocal);

            var merged = as(prune(parentTopN), TopN.class);

            assertFalse("merged TopN must stay a pipeline breaker if either input was one", merged.local());
            assertThat(merged.child(), sameInstance(source));
            assertThat(merged.limit(), equalTo(L(10)));
        }
    }

    public void testLocalTopNsMergeIntoLocalTopN() {
        var source = emptySource();
        var childTopN = new TopN(EMPTY, source, ORDER, L(100), true);
        var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(10), true);

        var merged = as(prune(parentTopN), TopN.class);

        assertTrue(merged.local());
        assertThat(merged.child(), sameInstance(source));
    }

    public void testLowerLimitIsKept() {
        var source = emptySource();
        var childTopN = new TopN(EMPTY, source, ORDER, L(10), false);
        var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(100), false);

        var merged = as(prune(parentTopN), TopN.class);

        assertThat(merged.limit(), equalTo(L(10)));
        assertThat(merged.child(), sameInstance(source));
    }

    public void testDifferentOrdersAreNotMerged() {
        var source = emptySource();
        var otherOrder = List.of(new Order(EMPTY, getFieldAttribute("b"), Order.OrderDirection.ASC, null));
        var childTopN = new TopN(EMPTY, source, otherOrder, L(100), false);
        var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(10), false);

        assertThat(prune(parentTopN), sameInstance(parentTopN));
    }
}
