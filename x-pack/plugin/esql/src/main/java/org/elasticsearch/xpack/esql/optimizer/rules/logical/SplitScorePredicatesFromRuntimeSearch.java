/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A runtime search that adds to {@code _score} only does so after the filter holding it has run (see
 * {@link FullTextFunction#containsRuntimeScorer}), so a {@code _score} predicate combined with it using {@code AND}
 * would see the score from before the search. This splits such predicates out into a filter above the search, where
 * they see its score; {@link PushDownAndCombineFilters} then keeps them there.
 * <p>
 * This has to run before anything substitutes {@code _score} for an alias or {@code RENAME} of it: a copy taken before
 * the search holds the score from before it, and once substituted it can't be told apart from a {@code _score} written
 * alongside the search. So it {@link #normalize}s the condition first, to expose an {@code AND} that optimization
 * would otherwise only expose after this rule has run. The verifier rejects any other mix of {@code _score} and a
 * runtime search, using the same normalization.
 * <p>
 * Whether a search is a runtime one isn't settled yet either, as a search on an alias of an indexed field looks like
 * one until push-down resolves the alias. Splitting such a filter does no harm: the search scores at the source, so
 * {@code _score} is final on both sides, and the two filters combine again once it is pushed down.
 */
public final class SplitScorePredicatesFromRuntimeSearch extends OptimizerRules.ParameterizedOptimizerRule<
    Filter,
    LogicalOptimizerContext> {

    private static final BooleanSimplification BOOLEAN_SIMPLIFICATION = new BooleanSimplification();

    public SplitScorePredicatesFromRuntimeSearch() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Filter filter, LogicalOptimizerContext ctx) {
        if (FullTextFunction.containsRuntimeScorer(filter.condition()) == false) {
            return filter;
        }
        Predicate<Expression> isScore = scoreOf(filter.child());
        if (filter.condition().anyMatch(isScore) == false) {
            return filter;
        }
        Expression normalized = normalize(filter.condition(), ctx.foldCtx());
        if (FullTextFunction.containsRuntimeScorer(normalized) == false) {
            // The search was simplified away, as in (MATCH(...) OR true) AND _score > 1.5, so there is nothing to split.
            return filter;
        }
        List<Expression> scorePredicates = new ArrayList<>();
        List<Expression> rest = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(normalized)) {
            // A conjunct that both reads _score and holds a runtime search can't be split; it stays with the search.
            if (conjunct.anyMatch(isScore) && FullTextFunction.containsRuntimeScorer(conjunct) == false) {
                scorePredicates.add(conjunct);
            } else {
                rest.add(conjunct);
            }
        }
        if (scorePredicates.isEmpty()) {
            return filter;
        }
        return filter.with(filter.with(filter.child(), Predicates.combineAnd(rest)), Predicates.combineAnd(scorePredicates));
    }

    /**
     * Matches the attributes that hold the score of {@code plan}'s rows: {@code _score} itself and, above a {@code FORK}
     * or {@code UNION ALL}, the output that carries it from a branch, where a runtime search pushed into that branch adds
     * to it. A copy of the score, such as an alias or {@code RENAME} of it, isn't one: it keeps the score from where it
     * was taken.
     */
    public static Predicate<Expression> scoreOf(LogicalPlan plan) {
        AttributeSet scores = branchScores(plan);
        return e -> MetadataAttribute.isScoreAttribute(e) || e instanceof Attribute attribute && scores.contains(attribute);
    }

    /**
     * {@code expression}, over the output of {@code merge}, as push-down rewrites it to go into {@code branch}.
     */
    public static Expression intoBranch(Expression expression, MergePlan merge, LogicalPlan branch) {
        Map<Expression, Expression> outputs = PushDownUtils.outputMap(merge, branch);
        return expression.transformDown(e -> outputs.getOrDefault(e, e));
    }

    /**
     * The attributes of {@code plan}'s output, other than {@code _score} itself, that carry the score of a branch below.
     */
    private static AttributeSet branchScores(LogicalPlan plan) {
        AttributeSet.Builder scores = AttributeSet.builder();
        if (plan instanceof MergePlan merge) {
            // Push-down maps the merge's output to each branch's by name.
            for (LogicalPlan branch : merge.children()) {
                Predicate<Expression> isBranchScore = scoreOf(branch);
                PushDownUtils.outputMap(merge, branch).forEach((output, branchOutput) -> {
                    if (isBranchScore.test(branchOutput)) {
                        // The keys are the merge's output attributes.
                        scores.add((Attribute) output);
                    }
                });
            }
        } else {
            AttributeSet.Builder fromChildren = AttributeSet.builder();
            plan.children().forEach(child -> fromChildren.addAll(branchScores(child)));
            for (Attribute output : plan.output()) {
                if (fromChildren.contains(output)) {
                    scores.add(output);
                }
            }
        }
        return scores.build();
    }

    /**
     * Folds boolean constants in {@code condition} and simplifies its boolean logic, as {@link ConstantFolding} and
     * {@link BooleanSimplification} later would, until neither changes it.
     */
    public static Expression normalize(Expression condition, FoldContext foldCtx) {
        Expression normalized = condition;
        Expression previous;
        do {
            previous = normalized;
            normalized = previous.transformDown(
                e -> e instanceof Literal == false && e.foldable() && e.dataType() == DataType.BOOLEAN ? Literal.of(foldCtx, e) : e
            );
            normalized = normalized.transformUp(ScalarFunction.class, BOOLEAN_SIMPLIFICATION::simplify);
        } while (normalized.equals(previous) == false);
        return normalized;
    }
}
