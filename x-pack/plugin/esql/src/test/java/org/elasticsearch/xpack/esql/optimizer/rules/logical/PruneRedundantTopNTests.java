/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
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
     * A non-local TopN is a pipeline breaker, so the merged TopN may only be local if both inputs are local; in particular,
     * a non-local TopN must survive merging regardless of whether it is the parent or the child.
     * Mixed locality is hard to reproduce through real query paths, hence the hand-built plans.
     */
    public void testMergedTopNIsLocalOnlyIfBothAreLocal() {
        var source = emptySource();
        for (boolean parentLocal : new boolean[] { true, false }) {
            for (boolean childLocal : new boolean[] { true, false }) {
                var childTopN = new TopN(EMPTY, source, ORDER, L(100), childLocal);
                var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(10), parentLocal);

                var merged = as(prune(parentTopN), TopN.class);

                assertThat(
                    "parent local=" + parentLocal + ", child local=" + childLocal,
                    merged.local(),
                    equalTo(parentLocal && childLocal)
                );
                assertThat(merged.child(), sameInstance(source));
                assertThat(merged.limit(), equalTo(L(10)));
            }
        }
    }

    public void testLowerLimitIsKeptWhenLowerInParent() {
        var source = emptySource();
        var childTopN = new TopN(EMPTY, source, ORDER, L(100), false);
        var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(10), false);

        var merged = as(prune(parentTopN), TopN.class);

        assertThat(merged.limit(), equalTo(L(10)));
        assertThat(merged.child(), sameInstance(source));
    }

    public void testLowerLimitIsKeptWhenLowerInChild() {
        var source = emptySource();
        var childTopN = new TopN(EMPTY, source, ORDER, L(10), false);
        var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(100), false);

        var merged = as(prune(parentTopN), TopN.class);

        assertThat(merged.limit(), equalTo(L(10)));
        assertThat(merged.child(), sameInstance(source));
    }

    public void testChainOfTopNsIsMergedIntoOne() {
        var source = emptySource();
        var innermost = new TopN(EMPTY, source, ORDER, L(50), false);
        var middle = new TopN(EMPTY, innermost, ORDER, L(100), false);
        var outermost = new TopN(EMPTY, middle, ORDER, L(75), false);

        var merged = as(prune(outermost), TopN.class);

        assertThat(merged.limit(), equalTo(L(50)));
        assertThat(merged.child(), sameInstance(source));
    }

    /**
     * Only a TopN directly on top of another TopN can be merged; any other child leaves the plan untouched.
     */
    public void testNoOpIfChildIsNotTopN() {
        var source = emptySource();
        var limit = new Limit(EMPTY, L(5), source);
        var topNOverLimit = new TopN(EMPTY, limit, ORDER, L(10), false);
        var topNOverRelation = new TopN(EMPTY, source, ORDER, L(10), false);
        // A TopN further down, separated by another node, must not be touched either.
        var topNOverLimitOverTopN = new TopN(
            EMPTY,
            new Limit(EMPTY, L(5), new TopN(EMPTY, source, ORDER, L(100), false)),
            ORDER,
            L(10),
            false
        );

        assertThat(prune(topNOverLimit), sameInstance(topNOverLimit));
        assertThat(prune(topNOverRelation), sameInstance(topNOverRelation));
        assertThat(prune(topNOverLimitOverTopN), sameInstance(topNOverLimitOverTopN));
    }

    public void testDifferentOrdersAreNotMerged() {
        var source = emptySource();
        var otherOrder = List.of(new Order(EMPTY, getFieldAttribute("b"), Order.OrderDirection.ASC, null));
        var childTopN = new TopN(EMPTY, source, otherOrder, L(100), false);
        var parentTopN = new TopN(EMPTY, childTopN, ORDER, L(10), false);

        assertThat(prune(parentTopN), sameInstance(parentTopN));
    }
}
