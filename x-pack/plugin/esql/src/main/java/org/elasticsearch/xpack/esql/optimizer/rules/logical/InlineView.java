/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MaterializedReadSource;
import org.elasticsearch.xpack.esql.plan.logical.RemoteViewSource;
import org.elasticsearch.xpack.esql.plan.logical.View;

/**
 * The boundary-aware view-lowering rule: a {@link View} survives analysis as a first-class node and this rule decides
 * <em>how</em> to lower it based on its {@link View.Boundary}, the three-way decision at the heart of the boundary-aware
 * optimizer.
 * <ul>
 *   <li><b>LOCAL</b> — fold the view into its body (the "peer-through" / Spark {@code EliminateView} strategy). Replacing
 *       the node with its resolved body lets the downstream pushdown rules optimize across the view, exactly the
 *       optimization today's early substitution achieves. This is the default and the only behaviour current views take.</li>
 *   <li><b>REMOTE</b> — keep the boundary opaque: the body must NOT execute locally, so lower to a {@link RemoteViewSource}
 *       leaf ("run this view's body on its home cluster") carrying the federation handle and the resolved schema.</li>
 *   <li><b>MATERIALIZED</b> — lower to a {@link MaterializedReadSource} leaf ("read the precomputed backing store")
 *       carrying the backing-index ref and the resolved schema.</li>
 * </ul>
 * The model is additive: a 4th boundary slots in by adding an enum constant + a branch here + a {@code Mapper} case,
 * leaving the existing three untouched.
 * <p>
 * Registered first in the operator-optimization batch of {@code LogicalPlanOptimizer}, before the pushdown rules so they
 * see through an inlined LOCAL view, and before {@code FlattenUnionAll} which lifts the nested {@code UnionAll}s that
 * folding a multi-source LOCAL view produces.
 */
public final class InlineView extends OptimizerRules.OptimizerRule<View> {

    @Override
    protected LogicalPlan rule(View view) {
        return switch (view.boundary()) {
            // In the parity phase the LOCAL View's pinned schema is exactly its body's output, so the fold is a direct
            // unwrap. Output reconciliation (a Project/cast when a declared view schema differs from the body's) lands
            // with a later phase.
            case LOCAL -> view.body();
            case REMOTE -> new RemoteViewSource(view.source(), view.viewName(), remoteHandle(view), view.output());
            case MATERIALIZED -> new MaterializedReadSource(view.source(), view.viewName(), backingIndex(view), view.output());
        };
    }

    private static String remoteHandle(View view) {
        View.LoweringTarget target = view.loweringTarget();
        if (target == null || target.handle() == null) {
            throw new EsqlIllegalArgumentException("REMOTE view [{}] has no remote-execution handle to lower", view.viewName());
        }
        return target.handle();
    }

    private static String backingIndex(View view) {
        View.LoweringTarget target = view.loweringTarget();
        if (target == null || target.backingIndex() == null) {
            throw new EsqlIllegalArgumentException("MATERIALIZED view [{}] has no backing index to lower", view.viewName());
        }
        return target.backingIndex();
    }
}
