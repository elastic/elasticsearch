/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Resolves {@link InSubquery} expressions in {@link Filter} conditions by rewriting them into
 * {@link SemiJoin}, {@link AntiJoin}, or {@link MarkJoin} nodes depending on where the
 * {@link InSubquery} sits inside the boolean expression:
 * <ul>
 *   <li>An {@code InSubquery} (optionally wrapped in {@link Not}) at the top of an AND-conjunct
 *       becomes a row-filtering {@link SemiJoin} / {@link AntiJoin} stacked on top of the
 *       remaining filter — the most efficient shape, used for the common conjunctive case.</li>
 *   <li>An {@code InSubquery} that appears as a child of {@link Or} (or of {@link Not} below an
 *       {@link Or}) is replaced with a synthetic boolean attribute and a {@link MarkJoin}
 *       is stacked below the rewritten {@link Filter}; the mark attribute carries the
 *       three-valued {@code IN} result up into normal boolean evaluation.</li>
 *   <li>An {@code InSubquery} wrapped in any other expression (a function argument, an
 *       {@code IS NOT NULL}, an arithmetic operator, etc.) is left in place; the post-resolution
 *       {@link #verify} step rejects the query with a {@link VerificationException}.</li>
 * </ul>
 * <p>
 * This runs before {@link PreAnalyzer} so the subquery plans, originally embedded inside
 * {@link InSubquery} expressions, become children of join nodes and visible to standard plan
 * traversals. This eliminates the need for separate InSubquery-aware traversals in
 * {@link PreAnalyzer}, {@link org.elasticsearch.xpack.esql.session.FieldNameUtils FieldNameUtils},
 * and {@link org.elasticsearch.xpack.esql.inference.InferenceService InferenceService}.
 * <p>
 * The join's {@code rightFields} are left empty at this stage because the subquery output is not
 * yet resolved. The Analyzer's {@code ResolveRefs} fills them in during the Resolution batch.
 */
public class InSubqueryResolver {

    /**
     * Resolves all {@link InSubquery} expressions in {@link Filter} conditions and validates the
     * result. Throws a {@link VerificationException} when an {@link InSubquery} survived rewriting
     * (e.g. inside an EVAL, SORT, STATS BY clause, or wrapped in a non-boolean expression).
     * <p>
     * Synchronous — does no I/O. Async callers should invoke this inside an
     * {@link org.elasticsearch.action.ActionListener#delegateFailureAndWrap delegateFailureAndWrap}
     * lambda so the thrown {@link VerificationException} is routed to {@code onFailure}.
     * <p>
     * Telemetry for {@code IN_SUBQUERY} is collected separately by the session — see
     * {@code EsqlSession#gatherInSubqueryMetrics}, which uses {@link #hasInSubqueryInFilter} on
     * the pre-resolution plan because by the time this method returns the originating
     * {@link InSubquery} expressions have been replaced with
     * {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin} and are no longer visible to plan
     * traversals. The {@code WHERE} counter still picks up SemiJoin/AntiJoin/MarkJoin in the
     * post-resolution plan walk (see {@code FeatureMetric#WHERE}), so the {@code WHERE} bit does
     * not need to be set up-front here.
     */
    public static LogicalPlan resolve(LogicalPlan plan) {
        LogicalPlan resolved = resolveInSubqueries(plan);
        verify(resolved);
        return resolved;
    }

    private static LogicalPlan resolveInSubqueries(LogicalPlan plan) {
        return plan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
    }

    /**
     * Returns {@code true} if the pre-resolution plan contains any {@link InSubquery} expression
     * inside a {@link Filter} (i.e. as part of a {@code WHERE} condition). Used by the session to
     * decide whether to increment the {@code IN_SUBQUERY} telemetry counter — once per query, in
     * the same spirit as {@code EsqlSession#gatherViewMetrics}.
     * <p>
     * Restricted to {@link Filter} conditions because {@link InSubquery} occurrences elsewhere
     * (EVAL, SORT, STATS BY, etc.) are rejected by {@link #verify} today.
     */
    public static boolean hasInSubqueryInFilter(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Filter filter && filter.condition().anyMatch(e -> e instanceof InSubquery));
    }

    /**
     * Spec for a {@link SemiJoin} / {@link AntiJoin} stacked on top of the remaining filter for
     * an {@link InSubquery} that appears as a top-level AND conjunct.
     */
    private record SemiOrAntiJoinSpec(Source source, LogicalPlan subquery, JoinConfig config, boolean anti) {}

    /**
     * Spec for a {@link MarkJoin} stacked below the remaining filter for an {@link InSubquery}
     * that appears under {@code OR}/{@code NOT}/{@code AND} but not as a top-level AND conjunct.
     * The mark attribute is referenced from the rewritten boolean expression.
     */
    private record MarkJoinSpec(Source source, LogicalPlan subquery, JoinConfig config, Attribute markAttribute) {}

    /**
     * Make this public, so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive IN subquery resolution.
     */
    public static LogicalPlan resolveInSubqueryInFilter(Filter filter) {
        Expression condition = filter.condition();

        List<Expression> conjuncts = Predicates.splitAnd(condition);

        List<Expression> remaining = new ArrayList<>();
        // Joins applied AFTER the remaining filter. SemiJoin/AntiJoin filter out rows that don't
        // satisfy the original IN/NOT IN predicate; they are correct only when the predicate is
        // an AND-conjunct.
        List<SemiOrAntiJoinSpec> semiOrAntiJoins = new ArrayList<>();
        // Joins applied BEFORE the remaining filter. MarkJoins emit a boolean mark attribute
        // referenced from the rewritten remaining condition; the mark carries the three-valued
        // IN result through the normal boolean evaluation in the surrounding OR/AND/NOT shape.
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        // Synthetic Eval aliases for constant left-hand side expressions (e.g. WHERE 10001 IN (subquery)).
        // Materialized as an Eval below the joins; the synthetic attributes are projected away above.
        List<Alias> syntheticEvals = new ArrayList<>();

        for (Expression conjunct : conjuncts) {
            if (tryResolveAsSemiOrAntiJoin(conjunct, semiOrAntiJoins, syntheticEvals)) {
                continue;
            }
            // Either no InSubquery in the conjunct (passes through unchanged), or InSubquery is
            // nested inside OR (rewritten with MarkJoin), or InSubquery sits under a
            // non-boolean wrapper (left as-is for {@link #verify} to reject).
            Expression rewritten = rewriteOrContextInSubqueries(conjunct, markJoins, syntheticEvals);
            remaining.add(rewritten);
        }

        if (semiOrAntiJoins.isEmpty() && markJoins.isEmpty()) {
            return filter;
        }

        LogicalPlan current = filter.child();

        // If any constants need materialization, insert an Eval to create the synthetic attributes.
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(filter.source(), current, syntheticEvals);
        }

        // Stack MarkJoins first — they are applied before the remaining filter so the mark
        // attributes are available to the rewritten boolean expression.
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source, current, mj.subquery, mj.config, mj.markAttribute);
        }

        // Apply remaining filter conditions on top of MarkJoins (so mark attributes are in scope).
        if (remaining.isEmpty() == false) {
            current = new Filter(filter.source(), current, Predicates.combineAnd(remaining));
        }

        // Stack SemiJoins / AntiJoins on top — they filter rows but don't modify columns.
        for (SemiOrAntiJoinSpec sj : semiOrAntiJoins) {
            current = sj.anti
                ? new AntiJoin(sj.source, current, sj.subquery, sj.config)
                : new SemiJoin(sj.source, current, sj.subquery, sj.config);
        }

        // The mark attributes from MarkJoins (and any synthetic constant Eval columns introduced
        // for foldable LHS) are flagged synthetic so the analyzer's default output projection
        // (planWithoutSyntheticAttributes) drops them — preserving the filter's apparent schema.
        return current;
    }

    /**
     * Attempts to handle {@code conjunct} as a top-level {@link InSubquery} (optionally wrapped in
     * one or more {@link Not}s) with an attribute or foldable LHS. On success appends the
     * corresponding {@link SemiOrAntiJoinSpec} (and any synthetic Eval Alias) and returns
     * {@code true}; otherwise returns {@code false} and leaves the accumulators untouched.
     */
    private static boolean tryResolveAsSemiOrAntiJoin(
        Expression conjunct,
        List<SemiOrAntiJoinSpec> semiOrAntiJoins,
        List<Alias> syntheticEvals
    ) {
        boolean negated = false;
        Expression expr = conjunct;
        while (expr instanceof Not not) {
            expr = not.field();
            negated = !negated;
        }

        if (expr instanceof InSubquery inSubquery) {
            Expression leftValue = inSubquery.value();
            List<Attribute> leftFields;
            if (leftValue instanceof Attribute leftAttr) {
                leftFields = singletonList(leftAttr);
            } else if (leftValue.foldable()) {
                var syntheticAlias = new Alias(
                    leftValue.source(),
                    syntheticConstName(leftValue, inSubquery.subquery()),
                    leftValue,
                    null,
                    true
                );
                syntheticEvals.add(syntheticAlias);
                leftFields = singletonList(syntheticAlias.toAttribute());
            } else {
                // Non-attribute, non-foldable LHS — leave it for the verifier to surface a clear error.
                return false;
            }

            LogicalPlan subquery = resolveNestedInSubqueries(inSubquery.subquery());
            JoinConfig config = new JoinConfig(negated ? JoinTypes.ANTI : JoinTypes.SEMI, leftFields, emptyList(), null);
            semiOrAntiJoins.add(new SemiOrAntiJoinSpec(inSubquery.source(), subquery, config, negated));
            return true;
        }
        return false;
    }

    /**
     * Walks the boolean expression replacing every {@link InSubquery} reachable through
     * {@link And}/{@link Or}/{@link Not} (i.e. boolean position) with a fresh synthetic mark
     * attribute, recording a {@link MarkJoinSpec} per replacement. {@link InSubquery}
     * occurrences that sit under a non-boolean wrapper (function argument, comparison, etc.) are
     * left in place for {@link #verify} to reject. Any expression with no eligible
     * {@link InSubquery} below it is returned unchanged.
     */
    private static Expression rewriteOrContextInSubqueries(Expression expr, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        if (expr instanceof And and) {
            Expression left = rewriteOrContextInSubqueries(and.left(), joins, syntheticEvals);
            Expression right = rewriteOrContextInSubqueries(and.right(), joins, syntheticEvals);
            return left == and.left() && right == and.right() ? and : new And(and.source(), left, right);
        }
        if (expr instanceof Or or) {
            Expression left = rewriteOrContextInSubqueries(or.left(), joins, syntheticEvals);
            Expression right = rewriteOrContextInSubqueries(or.right(), joins, syntheticEvals);
            return left == or.left() && right == or.right() ? or : new Or(or.source(), left, right);
        }
        if (expr instanceof Not not) {
            Expression child = rewriteOrContextInSubqueries(not.field(), joins, syntheticEvals);
            return child == not.field() ? not : new Not(not.source(), child);
        }
        if (expr instanceof InSubquery inSubquery) {
            return rewriteAsMarkJoin(inSubquery, joins, syntheticEvals);
        }
        // Non-boolean expression (function call, comparison, IS NOT NULL, etc.). Do NOT recurse:
        // any nested InSubquery should be reported as unsupported by the verifier rather than
        // silently lifted out into a join that would change the expression's semantics.
        return expr;
    }

    /**
     * Allocates a synthetic boolean mark attribute for {@code inSubquery}, records a
     * {@link MarkJoinSpec}, and returns the mark attribute as the replacement expression.
     * Returns the original {@link InSubquery} unchanged when the LHS is neither an attribute
     * nor foldable — those cases are surfaced as errors by {@link #verify}.
     */
    private static Expression rewriteAsMarkJoin(InSubquery inSubquery, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        Expression leftValue = inSubquery.value();
        List<Attribute> leftFields;
        if (leftValue instanceof Attribute leftAttr) {
            leftFields = singletonList(leftAttr);
        } else if (leftValue.foldable()) {
            var syntheticAlias = new Alias(leftValue.source(), syntheticConstName(leftValue, inSubquery.subquery()), leftValue, null, true);
            syntheticEvals.add(syntheticAlias);
            leftFields = singletonList(syntheticAlias.toAttribute());
        } else {
            return inSubquery;
        }

        LogicalPlan subquery = resolveNestedInSubqueries(inSubquery.subquery());
        Attribute markAttribute = new ReferenceAttribute(
            inSubquery.source(),
            null,
            syntheticMarkName(inSubquery),
            DataType.BOOLEAN,
            Nullability.TRUE,
            new NameId(),
            true
        );
        JoinConfig config = new JoinConfig(JoinTypes.MARK, leftFields, emptyList(), null);
        joins.add(new MarkJoinSpec(inSubquery.source(), subquery, config, markAttribute));
        return markAttribute;
    }

    /**
     * Recursively transforms a subquery plan, converting any nested IN/NOT IN subquery expressions
     * into SemiJoin/AntiJoin/MarkJoin nodes. This is needed because nested subquery plans are
     * embedded inside InSubquery expressions and not reachable by the top-level transformUp.
     */
    private static LogicalPlan resolveNestedInSubqueries(LogicalPlan subqueryPlan) {
        return subqueryPlan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
    }

    /**
     * Generates a unique synthetic name for a constant on the left-hand side of an IN subquery.
     */
    private static String syntheticConstName(Expression value, LogicalPlan subquery) {
        return "$$in_subquery_const$" + value.hashCode() + "$" + subquery.hashCode();
    }

    /**
     * Generates a unique synthetic name for the boolean mark attribute produced by a
     * {@link MarkJoin} in place of an {@link InSubquery}.
     */
    private static String syntheticMarkName(InSubquery inSubquery) {
        return "$$in_subquery_mark$" + inSubquery.value().hashCode() + "$" + inSubquery.subquery().hashCode();
    }

    public static void verify(LogicalPlan plan) {
        Failures failures = new Failures();
        checkInSubqueryUsage(plan, failures);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
    }

    private static void checkInSubqueryUsage(LogicalPlan plan, Failures failures) {
        plan.forEachDown(p -> {
            if (p instanceof Filter filter) {
                checkInFilterCondition(filter, filter.condition(), null, failures);
            } else {
                p.forEachExpression(
                    InSubquery.class,
                    inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", p.sourceText()))
                );
            }
        });
    }

    /**
     * Walks the {@code WHERE} condition tree to validate IN subquery usage that the
     * {@link InSubqueryResolver} could not rewrite into a {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin}.
     * <p>
     * If the IN subquery sits at the top of the boolean condition (i.e. only {@link And} /
     * {@link Or} / {@link Not} above it) the resolver normally rewrites it; if one survives here
     * it means the surrounding boolean shape is not yet supported (e.g. an unsupported LHS shape).
     * In that case we report the whole filter source (the entire {@code WHERE} clause).
     * <p>
     * Otherwise (the IN subquery is nested inside a non-boolean expression such as a scalar
     * function or {@code IS NOT NULL}), we report the immediately enclosing expression.
     */
    private static void checkInFilterCondition(Filter filter, Expression expr, Expression outerExpr, Failures failures) {
        if (expr instanceof InSubquery in) {
            if (outerExpr == null) {
                failures.add(fail(in, "Complicated IN subquery is not yet supported in the WHERE command [{}]", filter.sourceText()));
            } else {
                failures.add(fail(in, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
        }
        Expression newOuterExpr = outerExpr == null
            && expr instanceof And == false
            && expr instanceof Or == false
            && expr instanceof Not == false ? expr : outerExpr;
        for (Expression child : expr.children()) {
            checkInFilterCondition(filter, child, newOuterExpr, failures);
        }
    }
}
