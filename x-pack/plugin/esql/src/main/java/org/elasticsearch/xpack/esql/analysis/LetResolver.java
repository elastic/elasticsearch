/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.plan.LetBinding;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Substitutes {@link LetBinding} names into the main query plan (and earlier binding bodies)
 * before view resolution begins.
 *
 * <h2>When to call</h2>
 * <p>Call {@link #resolve} immediately before
 * {@link org.elasticsearch.xpack.esql.view.ViewResolver#replaceViews} in
 * {@code EsqlSession.execute}. This ordering gives the following properties for free:</p>
 * <ul>
 *   <li>Works in release builds and on clusters where the views feature is disabled — resolution
 *       is synchronous and does not touch cluster state or {@code ViewResolver}.</li>
 *   <li>View bodies never see the caller's {@code LET} scope — {@code parseView} is invoked
 *       inside {@code replaceViews}, after this pass has completed.</li>
 *   <li>A {@code LET} body may itself reference a stored view — the subsequent
 *       {@code replaceViews} call expands it.</li>
 *   <li>A {@code LET} body may contain an {@code IN} subquery — {@link InSubqueryResolver}
 *       runs inside {@code replaceViews}, after this pass, and sees the substituted plan.</li>
 *   <li>A {@code LET} name that shadows an index, alias, or view of the same name wins silently
 *       — substitution precedes both the view lookup and field-caps.</li>
 *   <li>A {@code LET} name never reaches field-caps — it is gone before
 *       {@link org.elasticsearch.xpack.esql.analysis.PreAnalyzer} runs.</li>
 * </ul>
 *
 * <h2>Scoping</h2>
 * <p>Bindings are evaluated left to right (sequential scoping): binding <em>N</em> sees
 * bindings <em>1..N-1</em> only. This makes true circular references structurally impossible
 * within a single pass — no cycle guard is needed.</p>
 *
 * <h2>One-pass substitution</h2>
 * <p>{@link #substitute} performs a <em>one-pass</em> substitution: every node that already
 * belongs to a resolved binding body is skipped when encountered during {@code transformDown}.
 * This is necessary because {@code transformDown} descends into newly introduced subtrees after
 * a replacement. Without this guard, a binding body such as {@code LET a = (FROM a | LIMIT 1)}
 * would cause infinite re-substitution: the inner {@code FROM a} (which refers to the ES index
 * {@code a}, not the LET binding) would be repeatedly replaced, causing a
 * {@link StackOverflowError}. The guard is identity-based so that only actual object instances
 * from resolved bodies are protected — fresh {@code UnresolvedRelation} objects in the original
 * plan are still substituted normally.</p>
 *
 */
public final class LetResolver {

    private LetResolver() {}

    /**
     * Substitutes {@code letBindings} names into {@code plan}, evaluating bindings in
     * declaration order so that later bodies can reference earlier names.
     *
     * @param plan        the parsed main query plan
     * @param letBindings the ordered list of {@code LET} bindings (may be empty)
     * @return the plan with all resolvable {@code LET} name references substituted
     */
    public static LogicalPlan resolve(LogicalPlan plan, List<LetBinding> letBindings) {
        if (letBindings.isEmpty()) {
            return plan;
        }

        // Left fold: build the resolved map incrementally so binding N sees bindings 1..N-1.
        Map<String, LogicalPlan> resolved = new LinkedHashMap<>(letBindings.size());
        for (LetBinding binding : letBindings) {
            // Substitute earlier bindings into this binding's body (sequential scoping).
            var current = substitute(binding.plan(), resolved);
            resolved.put(binding.name(), current);
            checkForCycles(current, resolved);
        }

        // Substitute the full map into the main query plan.
        var result = substitute(plan, resolved);
        checkForCycles(result, resolved);
        return result;
    }

    /**
     * Replaces every {@link UnresolvedRelation} in {@code plan} whose index-pattern string exactly
     * matches a key in {@code resolved} with the corresponding bound plan, without descending into
     * the replacement. Any node that is already part of a resolved body is skipped via an
     * identity-based guard to prevent infinite re-expansion (see class-level Javadoc).
     * Also substitutes into subquery plans embedded in {@link InSubquery} and
     * {@link MultiColumnInSubquery} expressions.
     */
    private static LogicalPlan substitute(LogicalPlan plan, Map<String, LogicalPlan> resolved) {
        if (resolved.isEmpty()) {
            return plan;
        }

        return plan.transformDown(p -> {
            if (p instanceof UnresolvedRelation ur) {
                String pattern = ur.indexPattern().indexPattern();
                LogicalPlan bound = resolved.get(pattern);
                return bound != null ? bound : ur;
            }
            // InSubquery and MultiColumnInSubquery carry a LogicalPlan field that is not part of the
            // plan-node children, so transformDown cannot reach it via the normal child traversal.
            // Substitute into those plans explicitly here.
            LogicalPlan result = p.transformExpressionsOnly(InSubquery.class, inSub -> {
                LogicalPlan newSubquery = substitute(inSub.subquery(), resolved);
                return newSubquery != inSub.subquery() ? new InSubquery(inSub.source(), inSub.value(), newSubquery) : inSub;
            });
            return result.transformExpressionsOnly(MultiColumnInSubquery.class, mcsub -> {
                LogicalPlan newSubquery = substitute(mcsub.subquery(), resolved);
                return newSubquery != mcsub.subquery() ? new MultiColumnInSubquery(mcsub.source(), mcsub.values(), newSubquery) : mcsub;
            });
        });
    }

    private static void checkForCycles(LogicalPlan plan, Map<String, LogicalPlan> resolved) {
        if (resolved.isEmpty()) {
            return;
        }

        plan.forEachDown(p -> {
            if (p instanceof UnresolvedRelation ur) {
                String pattern = ur.indexPattern().indexPattern();
                if (resolved.containsKey(pattern)) {
                    throw new VerificationException("Circular reference detected in LET bindings");
                }
            }
            // InSubquery and MultiColumnInSubquery carry a LogicalPlan field that is not part of the
            // plan-node children, so transformDown cannot reach it via the normal child traversal.
            // Substitute into those plans explicitly here.
            p.forEachExpression(InSubquery.class, inSub -> { checkForCycles(inSub.subquery(), resolved); });
            p.forEachExpression(MultiColumnInSubquery.class, mcsub -> { checkForCycles(mcsub.subquery(), resolved); });
        });
    }

}
