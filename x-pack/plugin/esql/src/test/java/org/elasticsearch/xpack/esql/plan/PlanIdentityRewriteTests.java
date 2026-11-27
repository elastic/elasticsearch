/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * What a rewrite does to the {@link PlanId} of the nodes it builds. The plan logs follow a node from one stage of planning to the
 * next by its id, so a rewrite of a node must keep it, and a node that was already in the plan must not be given another's.
 */
public class PlanIdentityRewriteTests extends ESTestCase {

    private static EsRelation relation() {
        return new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
    }

    /** A rule that builds a new node of the same class in place of the old one gives it the old one's identity. */
    public void testAFreshNodeAdoptsTheIdentityOfTheNodeItReplaces() {
        Filter filter = new Filter(Source.EMPTY, relation(), Literal.TRUE);

        Filter rewritten = (Filter) filter.transformDown(Filter.class, f -> new Filter(f.source(), f.child(), Literal.FALSE));

        assertThat(rewritten.condition(), equalTo(Literal.FALSE));
        assertThat(rewritten.planId(), equalTo(filter.planId()));
    }

    /** A rule that removes a node by returning its child leaves the child with its own identity, not the removed node's. */
    public void testACollapseOntoTheChildKeepsTheChildsIdentity() {
        Filter inner = new Filter(Source.EMPTY, relation(), Literal.TRUE);
        Filter outer = new Filter(Source.EMPTY, inner, Literal.TRUE);

        LogicalPlan collapsed = outer.transformDown(Filter.class, f -> f == outer ? f.child() : f);

        assertThat(collapsed, equalTo(inner));
        assertThat(collapsed.planId(), equalTo(inner.planId()));
        assertThat(collapsed.planId(), not(equalTo(outer.planId())));
    }

    /** Every node of a plan whose nodes are all rewritten keeps its own identity. */
    public void testEveryNodeOfARewrittenPlanKeepsItsIdentity() {
        LogicalPlan plan = relation();
        for (int i = 0; i < 20; i++) {
            plan = new Filter(Source.EMPTY, plan, Literal.TRUE);
        }
        List<PlanId> before = plan.collect(LogicalPlan.class::isInstance).stream().map(LogicalPlan::planId).toList();

        LogicalPlan rewritten = plan.transformUp(Filter.class, f -> new Filter(f.source(), f.child(), f.condition()));

        assertThat(rewritten.collect(LogicalPlan.class::isInstance).stream().map(LogicalPlan::planId).toList(), equalTo(before));
    }
}
