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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
 *   <li>An {@code InSubquery} inside a {@code STATS} {@code WHERE} filter (a {@link FilteredExpression}
 *       on an {@link Aggregate}) is replaced with a synthetic boolean attribute and a {@link MarkJoin}
 *       is stacked below the aggregate's child — MarkJoin-only. INLINE STATS is not supported.</li>
 *   <li>An {@code InSubquery} wrapped in any other expression (a comparison operator, an arithmetic
 *       operator, a lambda, etc.) is left in place; the post-resolution {@link #verify} step rejects
 *       the query with a {@link VerificationException}.</li>
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
     * Resolves all {@link InSubquery} expressions in {@link Filter} conditions and {@code STATS}
     * {@code WHERE} filters and validates the result. Throws a {@link VerificationException}
     * when an {@link InSubquery} survived rewriting.
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
        return resolveInSubqueries(plan, false);
    }

    /**
     * Apply {@link #resolveInSubqueryInFilter} to every {@link Filter} and {@link #resolveInSubqueryInAggregate} to every
     * {@link Aggregate} except the one owned by an {@link InlineStats} (INLINE STATS will be supported in a follow-up PR).
     */
    private static LogicalPlan resolveInSubqueries(LogicalPlan plan, boolean ownedByInlineStats) {
        boolean childOwnedByInlineStats = plan instanceof InlineStats;
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = null;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan resolved = resolveInSubqueries(child, childOwnedByInlineStats);
            if (resolved != child) {
                if (newChildren == null) {
                    newChildren = new ArrayList<>(children);
                }
                newChildren.set(i, resolved);
            }
        }
        LogicalPlan p = newChildren == null ? plan : plan.replaceChildrenSameSize(newChildren);
        if (p instanceof Filter filter) {
            return resolveInSubqueryInFilter(filter);
        }
        if (ownedByInlineStats == false && p instanceof Aggregate aggregate) {
            return resolveInSubqueryInAggregate(aggregate);
        }
        return p;
    }

    /**
     * Returns {@code true} if the pre-resolution plan contains any {@link InSubquery} expression inside a {@link Filter}
     * (i.e. as part of a {@code WHERE} condition) or inside the {@link FilteredExpression} filter of a {@code STATS} aggregate
     * (i.e. a per-aggregate {@code WHERE} filter). Used by the session to decide whether to increment the {@code IN_SUBQUERY} telemetry
     * counter — once per query, in the same spirit as {@code EsqlSession#gatherViewMetrics} — and by
     * {@link org.elasticsearch.xpack.esql.view.ViewResolver} to short-circuit resolution when there is nothing to rewrite.
     * <p>
     * Restricted to these two positions because {@link InSubquery} occurrences elsewhere (EVAL, SORT etc.) are rejected by {@link #verify}
     * today.
     */
    public static boolean hasInSubqueryInFilter(LogicalPlan plan) {
        return plan.anyMatch(
            p -> (p instanceof Filter filter && filter.condition().anyMatch(e -> e instanceof InSubquery))
                || (p instanceof Aggregate aggregate && hasInSubqueryInAggregateFilter(aggregate))
        );
    }

    private static boolean hasInSubqueryInAggregateFilter(Aggregate aggregate) {
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias
                && alias.child() instanceof FilteredExpression filteredExpression
                && filteredExpression.filter().anyMatch(e -> e instanceof InSubquery)) {
                return true;
            }
        }
        return false;
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
     * Rewrites {@link InSubquery} occurrences in the per-aggregate {@code WHERE} filters of a {@code STATS} {@link Aggregate} into
     * {@link MarkJoin}s stacked below the aggregate's child; the synthetic boolean mark attributes replace the {@link InSubquery}
     * occurrences inside the {@link FilteredExpression} filters. MarkJoin-only: a {@link SemiJoin}/{@link AntiJoin} would filter rows
     * feeding ALL aggregates (and, under {@code BY}, drop whole groups instead of producing empty-input aggregate values), while the
     * inline filter belongs to a single aggregate.
     * <p>
     * The mark attributes are consumed by the aggregate filters and do not leak into the output — an {@link Aggregate}'s output is its
     * aggregates plus groupings. Returns the same instance when nothing was rewritten (callers rely on identity).
     * <p>
     * Public so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive IN subquery resolution.
     */
    public static LogicalPlan resolveInSubqueryInAggregate(Aggregate aggregate) {
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        List<Alias> syntheticEvals = new ArrayList<>();
        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof FilteredExpression filteredExpression) {
                Expression newFilter = rewriteOrContextInSubqueries(filteredExpression.filter(), markJoins, syntheticEvals);
                if (newFilter != filteredExpression.filter()) {
                    ne = alias.replaceChild(new FilteredExpression(filteredExpression.source(), filteredExpression.delegate(), newFilter));
                }
            }
            newAggregates.add(ne);
        }

        if (markJoins.isEmpty()) {
            return aggregate;
        }

        LogicalPlan child = aggregate.child();
        // If any constants need materialization, insert an Eval to create the synthetic attributes.
        if (syntheticEvals.isEmpty() == false) {
            child = new Eval(aggregate.source(), child, syntheticEvals);
        }
        for (MarkJoinSpec mj : markJoins) {
            child = new MarkJoin(mj.source, child, mj.subquery, mj.config, mj.markAttribute);
        }
        return aggregate.with(child, aggregate.groupings(), newAggregates);
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
     * Walks the boolean expression replacing every {@link InSubquery} reachable through boolean-composing nodes with a fresh synthetic
     * mark attribute, recording a {@link MarkJoinSpec} per replacement. The boolean-composing nodes are:
     * <ul>
     *   <li>{@link And}, {@link Or}, {@link Not} — standard boolean connectives.</li>
     *   <li>{@link IsNull}, {@link IsNotNull} — {@code (x IN (sub)) IS [NOT] NULL}; the operand is a {@code valueExpression} in the
     *       grammar so it must be parenthesized, but the  resulting {@link IsNull}/{@link IsNotNull} node wraps the {@link InSubquery}
     *       directly.</li>
     *   <li>An {@link UnresolvedFunction} whose name is {@code CASE} or {@code COALESCE} (case-insensitive): every argument position may
     *       contain an {@link InSubquery} because all {@code functionParam} grammar alternatives accept a full {@code booleanExpression}.
     *       Note: at this stage the plan is pre-analysis, so these appear as {@link UnresolvedFunction}, not as the resolved
     *       {@code Case}/{@code Coalesce} classes.</li>
     * </ul>
     * {@link InSubquery} occurrences under any other wrapper (arithmetic, comparison, lambda, etc.) are left in place for {@link #verify}
     * to reject. Any expression with no eligible {@link InSubquery} below it is returned unchanged.
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
        if (isEligibleFunctionForInSubqueryRewrite(expr)) {
            List<Expression> children = expr.children();
            List<Expression> rewritten = new ArrayList<>(children.size());
            boolean changed = false;
            for (Expression child : children) {
                Expression r = rewriteOrContextInSubqueries(child, joins, syntheticEvals);
                rewritten.add(r);
                changed |= r != child;
            }
            return changed ? expr.replaceChildren(rewritten) : expr;
        }
        return expr;
    }

    /**
     * Returns {@code true} if {@code expr} is a boolean-composing expression whose children may be freely rewritten with {@link MarkJoin}
     * substitutions without changing semantics — i.e. an explicit allowlist of wrappers for which hoisting an {@link InSubquery} into a
     * join below the {@link Filter} is safe.
     * <p>
     * This is an allowlist, not "recurse into everything", so that lambdas and other constructs where the {@link InSubquery} LHS
     * references an in-scope parameter are kept out.
     */
    private static boolean isEligibleFunctionForInSubqueryRewrite(Expression expr) {
        if (expr instanceof IsNull || expr instanceof IsNotNull) {
            return true;
        }
        if (expr instanceof UnresolvedFunction uf) {
            String lowerName = uf.name().toLowerCase(Locale.ROOT);
            return lowerName.equals("case") || lowerName.equals("coalesce");
        }
        return false;
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
     * Recursively transforms any nested IN/NOT IN subquery expressions into SemiJoin/AntiJoin/MarkJoin nodes. This is needed because
     * nested subquery plans are embedded inside InSubquery expressions and not reachable by the top-level traversal.
     */
    private static LogicalPlan resolveNestedInSubqueries(LogicalPlan subqueryPlan) {
        return resolveInSubqueries(subqueryPlan);
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
        // Aggregates owned by an InlineStats keep the blanket rejection below; the identity set is safe because forEachDown is
        // pre-order (the InlineStats is visited before its child Aggregate) and nothing is rewritten during verification.
        Set<LogicalPlan> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.forEachDown(p -> {
            if (p instanceof InlineStats inlineStats) {
                inlineStatsAggregates.add(inlineStats.aggregate());
            }
            if (p instanceof Filter filter) {
                checkCondition(
                    filter.condition(),
                    null,
                    failures,
                    "Complicated IN subquery is not yet supported in the WHERE command [{}]",
                    filter.sourceText()
                );
            } else if (p instanceof Aggregate aggregate && inlineStatsAggregates.contains(aggregate) == false) {
                checkInAggregate(aggregate, failures);
            } else {
                p.forEachExpression(
                    InSubquery.class,
                    inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", p.sourceText()))
                );
            }
        });
    }

    /**
     * Validates IN subquery usage inside a {@code STATS} {@link Aggregate}: only the {@link FilteredExpression} filters (the
     * per-aggregate {@code WHERE} clauses) support IN subqueries — those are walked with {@link #checkCondition} to surface leftovers
     * that {@link #resolveInSubqueryInAggregate} could not rewrite. {@link InSubquery} anywhere else (groupings, aggregate function
     * arguments) keeps the blanket rejection.
     */
    private static void checkInAggregate(Aggregate aggregate, Failures failures) {
        for (Expression grouping : aggregate.groupings()) {
            grouping.forEachDown(
                InSubquery.class,
                inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", aggregate.sourceText()))
            );
        }
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof FilteredExpression filteredExpression) {
                filteredExpression.delegate()
                    .forEachDown(
                        InSubquery.class,
                        inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", aggregate.sourceText()))
                    );
                checkCondition(
                    filteredExpression.filter(),
                    null,
                    failures,
                    "Complicated IN subquery is not yet supported in the aggregate WHERE clause [{}]",
                    filteredExpression.sourceText()
                );
            } else {
                ne.forEachDown(
                    InSubquery.class,
                    inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", aggregate.sourceText()))
                );
            }
        }
    }

    /**
     * Walks a {@code WHERE} condition tree (a {@link Filter} condition or a {@code STATS} per-aggregate filter) to validate IN subquery
     * usage that the {@link InSubqueryResolver} could not rewrite into a {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin}.
     * <p>
     * If the IN subquery sits at the top of the boolean condition (i.e. only {@link And}, {@link Or}, {@link Not}, {@link IsNull},
     * {@link IsNotNull}, {@code CASE} or {@code COALESCE} above it) the resolver normally rewrites it; if one survives here it means
     * the LHS of the subquery is not yet supported (e.g. a non-attribute, non-foldable expression like {@code abs(x)}). In that case we
     * report {@code complicatedMessage} with {@code sourceText} (the entire {@code WHERE} clause or the aggregate's filtered expression).
     * <p>
     * Otherwise (the IN subquery is nested inside an expression that is not in the supported allowlist), we report the immediately
     * enclosing expression.
     */
    private static void checkCondition(
        Expression expr,
        Expression outerExpr,
        Failures failures,
        String complicatedMessage,
        String sourceText
    ) {
        if (expr instanceof InSubquery in) {
            if (outerExpr == null) {
                failures.add(fail(in, complicatedMessage, sourceText));
            } else {
                failures.add(fail(in, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
        }
        Expression newOuterExpr = outerExpr == null
            && expr instanceof And == false
            && expr instanceof Or == false
            && expr instanceof Not == false
            && isEligibleFunctionForInSubqueryRewrite(expr) == false ? expr : outerExpr;
        for (Expression child : expr.children()) {
            checkCondition(child, newOuterExpr, failures, complicatedMessage, sourceText);
        }
    }
}
