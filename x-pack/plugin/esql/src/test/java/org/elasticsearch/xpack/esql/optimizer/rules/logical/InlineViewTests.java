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
import org.elasticsearch.xpack.esql.plan.logical.MaterializedReadSource;
import org.elasticsearch.xpack.esql.plan.logical.RemoteViewSource;
import org.elasticsearch.xpack.esql.plan.logical.View;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit coverage for the boundary-aware {@link InlineView} lowering in isolation (its end-to-end wiring in
 * {@code LogicalPlanOptimizer} is exercised elsewhere). Pins the three-way decision: a LOCAL {@code View} folds to
 * exactly its body (parity), a REMOTE one lowers to an opaque {@code RemoteViewSource} (does NOT inline), a MATERIALIZED
 * one to a {@code MaterializedReadSource}; and a plan with no {@code View} is returned untouched.
 */
public class InlineViewTests extends ESTestCase {

    public void testFoldsViewToItsBody() {
        EsRelation body = relation();
        View view = new View(Source.EMPTY, "my_view", body);

        LogicalPlan folded = new InlineView().apply(view);

        assertThat(folded, sameInstance((LogicalPlan) body));
    }

    public void testLeavesNonViewPlansUnchanged() {
        EsRelation plan = relation();

        LogicalPlan result = new InlineView().apply(plan);

        assertThat(result, sameInstance((LogicalPlan) plan));
    }

    public void testRemoteViewDoesNotInlineAndLowersToRemoteSource() {
        EsRelation body = relation();
        View view = new View(Source.EMPTY, "remote_view", body, View.Boundary.REMOTE, View.LoweringTarget.remote("home_cluster"));

        LogicalPlan lowered = new InlineView().apply(view);

        // The body must NOT execute locally: the result is an opaque RemoteViewSource, not the body.
        assertThat(lowered, instanceOf(RemoteViewSource.class));
        assertThat(lowered, not(sameInstance((LogicalPlan) body)));
        RemoteViewSource remote = (RemoteViewSource) lowered;
        assertThat(remote.viewName(), equalTo("remote_view"));
        assertThat(remote.handle(), equalTo("home_cluster"));
        // Only the resolved schema survives the boundary.
        assertThat(remote.output(), equalTo(view.output()));
    }

    public void testMaterializedViewLowersToMaterializedReadSource() {
        EsRelation body = relation();
        View view = new View(
            Source.EMPTY,
            "mat_view",
            body,
            View.Boundary.MATERIALIZED,
            View.LoweringTarget.materialized(".materialized-mat_view")
        );

        LogicalPlan lowered = new InlineView().apply(view);

        assertThat(lowered, instanceOf(MaterializedReadSource.class));
        assertThat(lowered, not(sameInstance((LogicalPlan) body)));
        MaterializedReadSource materialized = (MaterializedReadSource) lowered;
        assertThat(materialized.viewName(), equalTo("mat_view"));
        assertThat(materialized.backingIndex(), equalTo(".materialized-mat_view"));
        assertThat(materialized.output(), equalTo(view.output()));
    }

    public void testRemoteViewWithoutHandleFails() {
        View view = new View(Source.EMPTY, "remote_view", relation(), View.Boundary.REMOTE, null);
        expectThrows(Exception.class, () -> new InlineView().apply(view));
    }
}
