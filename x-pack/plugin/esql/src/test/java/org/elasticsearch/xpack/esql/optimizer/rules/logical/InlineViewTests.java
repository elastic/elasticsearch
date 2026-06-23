/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.View;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit coverage for the {@link InlineView} fold in isolation (the rule is not yet wired into {@code LogicalPlanOptimizer}).
 * Pins the two invariants the parity wiring will rely on: a {@code View} folds to exactly its body, and a plan with no
 * {@code View} is returned untouched.
 */
public class InlineViewTests extends ESTestCase {

    public void testFoldsViewToItsBody() {
        EsRelation body = relation();
        View view = new View(Source.EMPTY, "my_view", body, body.output());

        LogicalPlan folded = new InlineView().apply(view);

        assertThat(folded, sameInstance((LogicalPlan) body));
    }

    public void testLeavesNonViewPlansUnchanged() {
        EsRelation plan = relation();

        LogicalPlan result = new InlineView().apply(plan);

        assertThat(result, sameInstance((LogicalPlan) plan));
    }
}
