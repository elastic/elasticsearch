/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LetBinding;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
 * <p>{@link #substitute} uses {@code transformDownSkipBranch}: when a replacement is made the
 * replacement subtree is not descended into. This enforces sequential scoping — a binding body
 * that was evaluated against the partial map (bindings declared before it) is not re-expanded
 * with later bindings when it appears as a replacement. It also prevents infinite re-substitution
 * for self-referential names: {@code LET a = (FROM a | LIMIT 1)} (where {@code FROM a} refers
 * to the ES index {@code a}) would otherwise loop indefinitely.
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
     * @param maxPlanNodesCount the maximum number of plan nodes the substituted plan, and each substituted binding, may reach
     * @return the plan with all resolvable {@code LET} name references substituted
     */
    public static LogicalPlan resolve(LogicalPlan plan, List<LetBinding> letBindings, int maxPlanNodesCount) {
        if (letBindings.isEmpty()) {
            return plan;
        }

        // Left fold: build the resolved map incrementally so binding N sees bindings 1..N-1.
        Map<String, LogicalPlan> resolved = new LinkedHashMap<>(letBindings.size());
        // Expanded node count of each resolved binding. Bound plans are inserted by reference, so counting them again on every
        // reference would be exponential for chained bindings; the sizes are tracked arithmetically instead.
        Map<String, Integer> sizes = new HashMap<>(letBindings.size());
        var allBindings = letBindings.stream().map(LetBinding::name).collect(Collectors.toSet());
        for (LetBinding binding : letBindings) {
            // Substitute earlier bindings into this binding's body (sequential scoping).
            PlanSize bindingSize = new PlanSize(PlanSize.countNodes(binding.plan()), maxPlanNodesCount);
            var current = substitute(binding.plan(), resolved, sizes, allBindings, bindingSize);
            resolved.put(binding.name(), current);
            sizes.put(binding.name(), bindingSize.size());
        }
        // Substitute the full map into the main query plan.
        return substitute(plan, resolved, sizes, allBindings, new PlanSize(PlanSize.countNodes(plan), maxPlanNodesCount));
    }

    /**
     * Replaces every {@link UnresolvedRelation} in {@code plan} whose index-pattern string exactly
     * matches a key in {@code resolved} with the corresponding bound plan, without descending into
     * the replacement. Any node that is already part of a resolved body is skipped via an
     * identity-based guard to prevent infinite re-expansion (see class-level Javadoc).
     * Also substitutes into subquery plans embedded in {@link InSubquery} and
     * {@link MultiColumnInSubquery} expressions.
     */
    private static LogicalPlan substitute(
        LogicalPlan plan,
        Map<String, LogicalPlan> resolved,
        Map<String, Integer> sizes,
        Set<String> allBindings,
        PlanSize planSize
    ) {
        // transformDownSkipBranch: when a replacement is made, set skipBranch so the replacement
        // subtree is not descended into. This enforces sequential scoping — a binding body that was
        // already evaluated against its own (partial) resolved map is not re-expanded with later
        // bindings when it appears as a replacement in the main query or an outer binding body.
        return plan.transformDownSkipBranch((p, skipBranch) -> {
            if (p instanceof UnresolvedRelation ur) {
                String indexPattern = ur.indexPattern().indexPattern();
                // Handle comma-joined patterns that contain one or more binding names as individual
                // tokens, e.g. "FROM top3, real_index" where top3 is a binding.
                // Split the indexPattern, substitute each token that matches a binding, and union the parts.
                String[] patterns = indexPattern.split(",", -1);
                boolean anyMatch = false;
                int addedNodes = 0;
                List<LogicalPlan> result = new ArrayList<>();
                List<String> nonBindingPatterns = new ArrayList<>();
                for (String pattern : patterns) {
                    String current = pattern.strip();
                    LogicalPlan bindingPlan = resolved.get(current);

                    if (bindingPlan != null) {
                        if (ur.indexMode().isTsdb()) {
                            // A binding is an ordinary subquery plan, so it cannot stand in for a time-series source: the
                            // parser already chose TS-specific planning for the surrounding commands. Mirrors the parser's
                            // rejection of inline subqueries in TS.
                            throw new ParsingException(ur.source(), "Subqueries are not supported in TS command");
                        }
                        if (ur.indexMode() == IndexMode.LOOKUP) {
                            // The right-hand side of a LOOKUP JOIN must be a lookup index, not an arbitrary plan; substituting a
                            // binding there would reach the physical planner as an unsupported join and fail with a 500.
                            throw new ParsingException(ur.source(), "Subqueries are not supported in LOOKUP JOIN");
                        }
                        anyMatch = true;
                        addedNodes += sizes.get(current);
                        result.add(bindingPlan);
                    } else {
                        // the query contained a reference to a binding that has not been resolved yet
                        if (allBindings.contains(current)) {
                            throw new VerificationException(
                                "Forward reference in LET bindings: [{}] cannot be referenced before its declaration",
                                current
                            );
                        }
                        nonBindingPatterns.add(current);
                    }
                }
                if (anyMatch) {
                    if (nonBindingPatterns.isEmpty() == false) {
                        result.add(
                            new UnresolvedRelation(
                                ur.source(),
                                new IndexPattern(ur.source(), String.join(",", nonBindingPatterns)),
                                ur.frozen(),
                                ur.metadataFields(),
                                ur.indexMode(),
                                null
                            )
                        );
                        nonBindingPatterns.clear();
                        addedNodes++;
                    }
                    LogicalPlan replacement;
                    if (result.size() > 1) {
                        addedNodes++;
                        replacement = new UnionAll(ur.source(), result, List.of());
                    } else {
                        LogicalPlan single = result.get(0);
                        // A single FROM a name is otherwise spliced in place. If the binding body already contains a FORK, keep a
                        // one-child UnionAll so checkForUnseparatedFork treats the LET scope as a merge boundary rather than consecutive
                        // FORKs on one pipeline.
                        if (Fork.containsFork(single)) {
                            addedNodes++;
                            replacement = new UnionAll(ur.source(), List.of(single), List.of());
                        } else {
                            replacement = single;
                        }
                    }
                    // The replaced relation is already part of the count.
                    planSize.add(addedNodes - 1);
                    skipBranch.set(true);
                    return replacement;
                }
                return ur;
            }
            // InSubquery and MultiColumnInSubquery carry a LogicalPlan field that is not part of the
            // plan-node children, so transformDownSkipBranch cannot reach it via the normal child
            // traversal. Substitute into those plans explicitly here.
            LogicalPlan result = p.transformExpressionsOnly(InSubquery.class, inSub -> {
                LogicalPlan newSubquery = substitute(inSub.subquery(), resolved, sizes, allBindings, planSize);
                return newSubquery != inSub.subquery() ? new InSubquery(inSub.source(), inSub.value(), newSubquery) : inSub;
            });
            return result.transformExpressionsOnly(MultiColumnInSubquery.class, mcsub -> {
                LogicalPlan newSubquery = substitute(mcsub.subquery(), resolved, sizes, allBindings, planSize);
                return newSubquery != mcsub.subquery() ? new MultiColumnInSubquery(mcsub.source(), mcsub.values(), newSubquery) : mcsub;
            });
        });
    }
}
