/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.View;

/**
 * Folds a {@link View} boundary into its body — the "peer-through" strategy. Replacing the leaf with its resolved body
 * lets the downstream pushdown rules optimize across the view, exactly the optimization today's early substitution
 * achieves, but now as a deliberate optimizer step over a first-class node rather than a pre-analysis rewrite. This is
 * Spark Catalyst's {@code EliminateView} analog, named for what it does; unlike Spark's it is meant to become
 * conditional (a remote / materialized / opaque view keeps its boundary and is lowered by the {@code Mapper} instead).
 * <p>
 * Placed before the pushdown rules so they see through an inlined view. Not yet registered in
 * {@code LogicalPlanOptimizer}: it is wired once resolution starts producing {@link View} nodes (then it runs by
 * default, preserving today's behaviour).
 */
public final class InlineView extends OptimizerRules.OptimizerRule<View> {

    @Override
    protected LogicalPlan rule(View view) {
        // In the parity phase the View's pinned schema is exactly its body's output, so the fold is a direct unwrap.
        // Output reconciliation (a Project/cast when a declared view schema differs from the body's) and the
        // keep-opaque alternative land with the boundary-aware lowering phase.
        return view.body();
    }
}
