/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes {@link Alias}es computed by an intra-fragment {@link EvalExec} that nothing above the
 * node still references, and drops the {@link EvalExec} entirely when none of its fields survive.
 * <p>
 * This complements the logical {@code PruneColumns} rule, which cannot see the residual
 * {@link EvalExec} that {@link PushFiltersToSource} leaves behind: once a {@code WHERE} that
 * referenced an {@code EVAL}-produced alias is fully pushed into Lucene, the alias may no longer be
 * used by anything downstream, yet the {@link EvalExec} computing it is rebuilt verbatim. That dead
 * compute shows up as a superfluous {@code EvalOperator} in the data-node driver.
 * <p>
 * The rule walks the local physical plan top-down (like {@code ProjectAwayColumns}), accumulating
 * the set of attributes required by everything above the current node, so it can decide — with the
 * whole-plan visibility that {@link PushFiltersToSource} lacks locally — whether each eval field is
 * still needed. It only removes dead aliases; it never re-runs pushdown, so it does not fight with
 * the "Push to ES" batch re-executing.
 */
public class PruneUnusedEvalFields extends Rule<PhysicalPlan, PhysicalPlan> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        // Invariant: requiredAttrBuilder holds every attribute referenced by nodes above (and beside)
        // the node currently being visited. transformDown visits parents before children, so at each
        // EvalExec this set already reflects everything downstream of it.
        AttributeSet.Builder requiredAttrBuilder = plan.outputSet().asBuilder();
        Holder<Boolean> changed = new Holder<>(Boolean.FALSE);

        PhysicalPlan transformed = plan.transformDown(currentPlanNode -> {
            if (currentPlanNode instanceof EvalExec evalExec) {
                List<Alias> remaining = pruneUnusedAndAddReferences(evalExec.fields(), requiredAttrBuilder);
                if (remaining == null) {
                    // Nothing pruned: still fold in this eval's references for nodes below it.
                    requiredAttrBuilder.addAll(evalExec.references());
                    return evalExec;
                }
                changed.set(Boolean.TRUE);
                if (remaining.isEmpty()) {
                    // No surviving fields: drop the EvalExec, keeping its child.
                    return evalExec.child();
                }
                return new EvalExec(evalExec.source(), evalExec.child(), remaining);
            }
            // For every other node, whatever it references is required by the nodes below it.
            requiredAttrBuilder.addAll(currentPlanNode.references());
            return currentPlanNode;
        });

        return changed.get() ? transformed : plan;
    }

    /**
     * Drops aliases from {@code fields} whose attribute is not present in {@code required}, walking
     * from last to first so that an earlier field feeding a later, surviving field is retained. As a
     * side effect the references of every kept alias are added to {@code required}. Returns
     * {@code null} when nothing was pruned.
     */
    private static List<Alias> pruneUnusedAndAddReferences(List<Alias> fields, AttributeSet.Builder required) {
        List<Alias> clone = new ArrayList<>(fields);
        for (var it = clone.listIterator(clone.size()); it.hasPrevious();) {
            Alias alias = it.previous();
            if (required.contains(alias.toAttribute())) {
                required.addAll(alias.references());
            } else {
                it.remove();
            }
        }
        return clone.size() != fields.size() ? clone : null;
    }
}
