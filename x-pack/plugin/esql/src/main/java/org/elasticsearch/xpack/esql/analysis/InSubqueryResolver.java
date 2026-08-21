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
import org.elasticsearch.xpack.esql.core.expression.Lambda;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
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
 *   <li>An {@code InSubquery} inside a {@link IsNull}/{@link IsNotNull} operand, or inside any
 *       argument of a {@code CASE} or {@code COALESCE} call, is replaced with a synthetic boolean
 *       attribute and a {@link MarkJoin} is stacked below the rewritten {@link Filter} —
 *       identical to the {@link Or} case above. These eligible expressions may themselves be
 *       nested inside comparisons, arithmetic operators, or other ordinary expressions.</li>
 *   <li>An {@code InSubquery} directly wrapped in any other expression, or nested inside a
 *       scope-bearing expression such as a lambda, is left in place; the post-resolution
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
            p -> (p instanceof Filter filter
                && filter.condition().anyMatch(e -> e instanceof InSubquery || e instanceof MultiColumnInSubquery))
                || (p instanceof Aggregate aggregate && hasInSubqueryInAggregateFilter(aggregate))
        );
    }

    private static boolean hasInSubqueryInAggregateFilter(Aggregate aggregate) {
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias
                && alias.child() instanceof FilteredExpression filteredExpression
                && filteredExpression.filter().anyMatch(e -> e instanceof InSubquery || e instanceof MultiColumnInSubquery)) {
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
            // nested inside a supported boolean expression (rewritten with MarkJoin), or
            // InSubquery sits directly under an unsupported wrapper (left for {@link #verify}).
            Expression rewritten = rewriteInSubqueries(conjunct, true, markJoins, syntheticEvals);
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
        List<? extends NamedExpression> origAggregates = aggregate.aggregates();
        List<NamedExpression> newAggregates = null;
        for (int i = 0; i < origAggregates.size(); i++) {
            NamedExpression ne = origAggregates.get(i);
            if (ne instanceof Alias alias && alias.child() instanceof FilteredExpression filteredExpression) {
                Expression newFilter = rewriteInSubqueries(filteredExpression.filter(), true, markJoins, syntheticEvals);
                if (newFilter != filteredExpression.filter()) {
                    ne = alias.replaceChild(new FilteredExpression(filteredExpression.source(), filteredExpression.delegate(), newFilter));
                    if (newAggregates == null) {
                        newAggregates = new ArrayList<>(origAggregates.size());
                        newAggregates.addAll(origAggregates.subList(0, i));
                    }
                }
            }
            if (newAggregates != null) {
                newAggregates.add(ne);
            }
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
        // Invariant: markJoins is non-empty iff at least one FilteredExpression filter was rewritten,
        // which is the only code path that initializes newAggregates.
        assert newAggregates != null;
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

        Source source;
        List<Attribute> leftFields;
        LogicalPlan subqueryPlan;
        if (expr instanceof InSubquery inSubquery) {
            leftFields = resolveSingleColumn(inSubquery.value(), inSubquery.subquery(), syntheticEvals);
            source = inSubquery.source();
            subqueryPlan = inSubquery.subquery();
        } else if (expr instanceof MultiColumnInSubquery mcs) {
            leftFields = resolveMultiColumn(mcs.values(), mcs.subquery(), syntheticEvals);
            source = mcs.source();
            subqueryPlan = mcs.subquery();
        } else {
            return false;
        }
        if (leftFields == null) {
            // Non-attribute, non-foldable LHS — leave it for the verifier to surface a clear error.
            return false;
        }
        LogicalPlan subquery = resolveNestedInSubqueries(subqueryPlan);
        JoinConfig config = new JoinConfig(negated ? JoinTypes.ANTI : JoinTypes.SEMI, leftFields, emptyList(), null);
        semiOrAntiJoins.add(new SemiOrAntiJoinSpec(source, subquery, config, negated));
        return true;
    }

    /**
     * Walks the expression replacing every {@link InSubquery} whose position in the tree is eligible for join-hoisting with a fresh
     * synthetic mark attribute, recording a {@link MarkJoinSpec} per replacement.
     * <p>
     * An {@link InSubquery} is eligible if its direct parent is one of the "eligible boolean-composing nodes":
     * <ul>
     *   <li>{@link And}, {@link Or}, {@link Not} — standard boolean connectives.</li>
     *   <li>{@link IsNull}, {@link IsNotNull} — e.g. {@code (x IN (sub)) IS [NOT] NULL}.</li>
     *   <li>An {@link UnresolvedFunction} whose name is {@code CASE} or {@code COALESCE} (case-insensitive).</li>
     * </ul>
     * <p>
     * The resolver uses a recursive traversal to find these nodes. When it encounters an eligible wrapper, it re-enables rewriting for
     * all its children. This allows subqueries to be resolved even when deeply nested, provided they are directly inside an eligible node.
     * <p>
     * For example, in {@code (CASE(x IN (sub), true, false) + 1) == 2}:
     * <ol>
     *   <li>The {@code ==} operator is an "ordinary" expression. It does not allow an immediate {@code InSubquery} child, but it is
     *       transparent to traversal, so it calls into its children with {@code rewriteCurrentInSubquery = false}.</li>
     *   <li>The {@code +} operator is also transparent and continues the traversal.</li>
     *   <li>The {@code CASE} function is an eligible wrapper. It calls into its arguments with {@code rewriteCurrentInSubquery = true}.
     *       </li>
     *   <li>The {@code InSubquery} is now eligible because its direct parent (the {@code CASE}) is an eligible wrapper. It is replaced
     *       with a mark attribute and a {@link MarkJoin} is recorded.</li>
     * </ol>
     * Conversely, in {@code (x IN (sub)) == true}, the {@code ==} operator does not enable rewriting for its children, so the subquery
     * remains unresolved and is later caught by {@link #verify}.
     * <p>
     * Scope-bearing expressions such as {@link Lambda} act as a rewrite boundary and stop the traversal entirely.
     */
    private static Expression rewriteInSubqueries(
        Expression expr,
        boolean rewriteCurrentInSubquery,
        List<MarkJoinSpec> joins,
        List<Alias> syntheticEvals
    ) {
        if (expr instanceof InSubquery inSubquery) {
            return rewriteCurrentInSubquery ? rewriteAsMarkJoin(inSubquery, joins, syntheticEvals) : inSubquery;
        }
        if (expr instanceof MultiColumnInSubquery inSubquery) {
            return rewriteCurrentInSubquery ? rewriteAsMarkJoin(inSubquery, joins, syntheticEvals) : inSubquery;
        }
        if (isInSubqueryRewriteBoundary(expr)) {
            return expr;
        }

        boolean rewriteChildren = canRewriteInSubqueryChildren(expr);
        List<Expression> children = expr.children();
        List<Expression> rewritten = null;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            Expression r = rewriteInSubqueries(child, rewriteChildren, joins, syntheticEvals);
            if (rewritten != null) {
                rewritten.add(r);
            } else if (r != child) {
                rewritten = new ArrayList<>(children.size());
                for (int j = 0; j < i; j++) {
                    rewritten.add(children.get(j));
                }
                rewritten.add(r);
            }
        }
        return rewritten != null ? expr.replaceChildren(rewritten) : expr;
    }

    /**
     * Returns whether an InSubquery directly below {@code expr} can be replaced with a mark attribute.
     */
    private static boolean canRewriteInSubqueryChildren(Expression expr) {
        if (expr instanceof And || expr instanceof Or || expr instanceof Not || expr instanceof IsNull || expr instanceof IsNotNull) {
            return true;
        }
        if (expr instanceof UnresolvedFunction uf) {
            String lowerName = uf.name().toLowerCase(Locale.ROOT);
            return lowerName.equals("case") || lowerName.equals("coalesce");
        }
        return false;
    }

    /**
     * Returns whether traversal must stop at {@code expr} because descendants may reference names scoped by the expression.
     */
    private static boolean isInSubqueryRewriteBoundary(Expression expr) {
        return expr instanceof Lambda;
    }

    /**
     * Allocates a synthetic boolean mark attribute for {@code inSubquery}, records a
     * {@link MarkJoinSpec}, and returns the mark attribute as the replacement expression.
     * Returns the original {@link InSubquery} unchanged when the LHS is neither an attribute
     * nor foldable — those cases are surfaced as errors by {@link #verify}.
     */
    private static Expression rewriteAsMarkJoin(InSubquery inSubquery, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        List<Attribute> leftFields = resolveSingleColumn(inSubquery.value(), inSubquery.subquery(), syntheticEvals);
        if (leftFields == null) {
            return inSubquery;
        }
        LogicalPlan subquery = resolveNestedInSubqueries(inSubquery.subquery());
        Attribute markAttribute = new ReferenceAttribute(
            inSubquery.source(),
            null,
            syntheticMarkName(inSubquery, joins.size()),
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
     * Allocates a synthetic boolean mark attribute for {@code mcs}, records a
     * {@link MarkJoinSpec}, and returns the mark attribute as the replacement expression.
     * Returns the original {@link MultiColumnInSubquery} unchanged when any LHS value is neither
     * an attribute nor foldable — those cases are surfaced as errors by {@link #verify}.
     */
    private static Expression rewriteAsMarkJoin(MultiColumnInSubquery mcs, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        List<Attribute> leftFields = resolveMultiColumn(mcs.values(), mcs.subquery(), syntheticEvals);
        if (leftFields == null) {
            return mcs;
        }
        LogicalPlan subquery = resolveNestedInSubqueries(mcs.subquery());
        Attribute markAttribute = new ReferenceAttribute(
            mcs.source(),
            null,
            syntheticMarkName(mcs, joins.size()),
            DataType.BOOLEAN,
            Nullability.TRUE,
            new NameId(),
            true
        );
        JoinConfig config = new JoinConfig(JoinTypes.MARK, leftFields, emptyList(), null);
        joins.add(new MarkJoinSpec(mcs.source(), subquery, config, markAttribute));
        return markAttribute;
    }

    /**
     * Recursively transforms a subquery plan, converting any nested IN/NOT IN subquery expressions
     * into SemiJoin/AntiJoin/MarkJoin nodes. This is needed because nested subquery plans are
     * embedded inside InSubquery expressions and not reachable by the top-level transformUp.
     */
    private static LogicalPlan resolveNestedInSubqueries(LogicalPlan subqueryPlan) {
        return resolveInSubqueries(subqueryPlan);
    }

    /**
     * Resolves the left-hand side value of an {@link InSubquery} into a singleton {@link Attribute} list suitable for a join config.
     * A foldable value is materialized as a synthetic {@link Alias} appended to {@code syntheticEvals}. Returns {@code null} if the value
     * is neither an attribute nor foldable (i.e. unsupported — left for the verifier to reject).
     */
    private static List<Attribute> resolveSingleColumn(Expression value, LogicalPlan subquery, List<Alias> syntheticEvals) {
        if (value instanceof Attribute attr) {
            return singletonList(attr);
        } else if (value.foldable()) {
            var syntheticAlias = new Alias(value.source(), syntheticConstName(value, subquery, syntheticEvals.size()), value, null, true);
            syntheticEvals.add(syntheticAlias);
            return singletonList(syntheticAlias.toAttribute());
        } else {
            return null;
        }
    }

    /**
     * Resolves the left-hand side values of a {@link MultiColumnInSubquery} into a list of {@link Attribute}s suitable for a join config.
     * Foldable values are materialized as synthetic, {@link Alias}es appended to {@code syntheticEvals}. Returns {@code null} if any value
     * is neither an attribute nor foldable (i.e. unsupported — left for the verifier to reject).
     */
    private static List<Attribute> resolveMultiColumn(List<Expression> values, LogicalPlan subquery, List<Alias> syntheticEvals) {
        List<Attribute> leftFields = new ArrayList<>(values.size());
        for (Expression value : values) {
            if (value instanceof Attribute attr) {
                leftFields.add(attr);
            } else if (value.foldable()) {
                var syntheticAlias = new Alias(
                    value.source(),
                    syntheticConstName(value, subquery, syntheticEvals.size()),
                    value,
                    null,
                    true
                );
                syntheticEvals.add(syntheticAlias);
                leftFields.add(syntheticAlias.toAttribute());
            } else {
                return null;
            }
        }
        return leftFields;
    }

    /**
     * Generates a unique synthetic name for a constant on the left-hand side of an IN subquery. The value and subquery hashes alone
     * cannot guarantee uniqueness: equal constants compared against the same subquery hash identically (e.g. the repeated {@code 1}s
     * in {@code WHERE (1, 1) IN (subquery)}, or the same {@code IN} predicate appearing twice in one {@code WHERE}). All synthetic
     * aliases of one {@link Filter} rewrite are materialized in a single {@link Eval}, whose output merging silently drops earlier
     * same-named fields — which would orphan the join key referencing the dropped alias — so {@code ordinal} (the alias's position in
     * the per-rewrite {@code syntheticEvals} list) is appended to keep the names unique.
     */
    private static String syntheticConstName(Expression value, LogicalPlan subquery, int ordinal) {
        return "$$in_subquery_const$" + value.hashCode() + "$" + subquery.hashCode() + "$" + ordinal;
    }

    /**
     * Generates a unique synthetic name for the boolean mark attribute produced by a {@link MarkJoin} in place of an {@link InSubquery}.
     * <p>
     * Value and subquery hashes alone cannot guarantee uniqueness: the same {@link InSubquery} appearing twice in one expression
     * (e.g. {@code x IN (sub) OR x IN (sub)}) produces two nodes with identical hashes. The {@code ordinal} (the join's position in the
     * per-rewrite join list at the time of creation) makes each name unique, mirroring the pattern used by {@link #syntheticConstName}.
     * TODO; consider a deduplication optimization as a future enhancement, so that {@code x IN (sub) OR x IN (sub)} produces only one
     *  join and one mark attribute.
     */
    private static String syntheticMarkName(InSubquery inSubquery, int ordinal) {
        return "$$in_subquery_mark$" + inSubquery.value().hashCode() + "$" + inSubquery.subquery().hashCode() + "$" + ordinal;
    }

    /**
     * Generates a unique synthetic name for the boolean mark attribute produced by a
     * {@link MarkJoin} in place of a {@link MultiColumnInSubquery}.
     * <p>
     * The {@code ordinal} disambiguates the case where the same predicate appears twice in one expression, for the same reason as the
     * single-column overload.
     * TODO: consider a deduplication optimization as a future enhancement, so that {@code (x, y) IN (sub) OR (x, y) IN (sub)} produces
     *  only one join and one mark attribute.
     */
    private static String syntheticMarkName(MultiColumnInSubquery mcs, int ordinal) {
        return "$$in_subquery_mark$" + mcs.hashCode() + "$" + ordinal;
    }

    public static void verify(LogicalPlan plan) {
        Failures failures = new Failures();
        checkInSubqueryUsage(plan, failures);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
    }

    private static void checkInSubqueryUsage(LogicalPlan plan, Failures failures) {
        // Collect InlineStats-owned Aggregates first so the validation pass below can skip them.
        Set<LogicalPlan> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.forEachDown(InlineStats.class, inlineStats -> inlineStatsAggregates.add(inlineStats.aggregate()));

        plan.forEachDown(p -> {
            if (p instanceof Filter filter) {
                checkInSubqueryExpression(filter, filter.condition(), true, false, null, failures);
            } else if (p instanceof Aggregate aggregate && inlineStatsAggregates.contains(aggregate) == false) {
                checkInAggregate(aggregate, failures);
            } else {
                p.forEachExpression(
                    InSubquery.class,
                    inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", p.sourceText()))
                );
                p.forEachExpression(
                    MultiColumnInSubquery.class,
                    mcs -> failures.add(fail(mcs, "IN subquery is not supported in [{}]", p.sourceText()))
                );
            }
        });
    }

    /**
     * Validates IN subquery usage inside a {@code STATS} {@link Aggregate}: only the {@link FilteredExpression} filters (the
     * per-aggregate {@code WHERE} clauses) support IN subqueries — those are walked with {@link #checkInSubqueryExpression} to surface
     * leftovers that {@link #resolveInSubqueryInAggregate} could not rewrite. {@link InSubquery} anywhere else (groupings, aggregate
     * function arguments) keeps the blanket rejection.
     */
    private static void checkInAggregate(Aggregate aggregate, Failures failures) {
        for (Expression grouping : aggregate.groupings()) {
            grouping.forEachDown(Expression.class, e -> rejectInSubquery(e, aggregate, failures));
        }
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof FilteredExpression filteredExpression) {
                filteredExpression.delegate().forEachDown(Expression.class, e -> rejectInSubquery(e, aggregate, failures));
                checkInSubqueryExpression(aggregate, filteredExpression.filter(), true, false, null, failures);
            } else {
                ne.forEachDown(Expression.class, e -> rejectInSubquery(e, aggregate, failures));
            }
        }
    }

    private static void rejectInSubquery(Expression e, Aggregate aggregate, Failures failures) {
        if (e instanceof InSubquery || e instanceof MultiColumnInSubquery) {
            failures.add(fail(e, "IN subquery is not supported in [{}]", aggregate.sourceText()));
        }
    }

    /**
     * If the IN subquery sits at the top of the boolean condition (i.e. only {@code And}, {@code Or}, {@code Not}, {@code IsNull},
     * {@code IsNotNull}, {@code CASE} or {@code COALESCE} above it) the resolver normally rewrites it; if one survives here it means
     * the LHS of the subquery is not yet supported (e.g. a non-attribute, non-foldable expression like {@code abs(x)}). In that case we
     * report {@code complicatedMessage} with {@code sourceText} (the entire {@code WHERE} clause or the aggregate's filtered expression).
     * <p>
     * Otherwise (the IN subquery is nested inside an expression that is not in the supported allowlist), we report the immediately
     * enclosing expression.
     */
    private static void checkInSubqueryExpression(
        LogicalPlan plan,
        Expression expr,
        boolean rewriteCurrentInSubquery,
        boolean insideRewriteBoundary,
        Expression outerExpr,
        Failures failures
    ) {
        if (expr instanceof InSubquery || expr instanceof MultiColumnInSubquery) {
            if (rewriteCurrentInSubquery && insideRewriteBoundary == false) {
                failures.add(fail(expr, "Complicated IN subquery is not yet supported in {} [{}]", plan.nodeName(), plan.sourceText()));
            } else {
                failures.add(fail(expr, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
            return;
        }

        boolean newInsideRewriteBoundary = insideRewriteBoundary || isInSubqueryRewriteBoundary(expr);
        boolean rewriteChildren = newInsideRewriteBoundary == false && canRewriteInSubqueryChildren(expr);
        Expression newOuterExpr = rewriteChildren ? null : newInsideRewriteBoundary && outerExpr != null ? outerExpr : expr;
        for (Expression child : expr.children()) {
            checkInSubqueryExpression(plan, child, rewriteChildren, newInsideRewriteBoundary, newOuterExpr, failures);
        }
    }
}
