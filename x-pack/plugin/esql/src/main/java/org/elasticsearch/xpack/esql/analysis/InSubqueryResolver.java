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
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
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
import java.util.Locale;

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
        return plan.anyMatch(
            p -> p instanceof Filter filter
                && filter.condition().anyMatch(e -> e instanceof InSubquery || e instanceof MultiColumnInSubquery)
        );
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
     * Returns whether an {@link InSubquery} directly below {@code expr} can be replaced with a mark attribute.
     */
    private static boolean canRewriteInSubqueryChildren(Expression expr) {
        return expr instanceof And || expr instanceof Or || expr instanceof Not || isEligibleFunctionForInSubqueryRewrite(expr);
    }

    /**
     * Returns whether traversal must stop at {@code expr} because descendants may reference names scoped by the expression.
     */
    private static boolean isInSubqueryRewriteBoundary(Expression expr) {
        return expr instanceof Lambda;
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
        List<Attribute> leftFields = resolveSingleColumn(inSubquery.value(), inSubquery.subquery(), syntheticEvals);
        if (leftFields == null) {
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
            syntheticMarkName(mcs),
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
        return subqueryPlan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
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
     * Generates a unique synthetic name for the boolean mark attribute produced by a
     * {@link MarkJoin} in place of an {@link InSubquery}.
     */
    private static String syntheticMarkName(InSubquery inSubquery) {
        return "$$in_subquery_mark$" + inSubquery.value().hashCode() + "$" + inSubquery.subquery().hashCode();
    }

    /**
     * Generates a unique synthetic name for the boolean mark attribute produced by a
     * {@link MarkJoin} in place of a {@link MultiColumnInSubquery}.
     */
    private static String syntheticMarkName(MultiColumnInSubquery mcs) {
        return "$$in_subquery_mark$" + mcs.hashCode();
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
                checkInFilterCondition(filter, filter.condition(), true, false, null, failures);
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
     * Walks the {@code WHERE} condition tree to validate IN subquery usage that the
     * {@link InSubqueryResolver} could not rewrite into a {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin}.
     * <p>
     * If the IN subquery is directly below an eligible boolean-composing expression, the resolver normally rewrites it; if one survives
     * here it means the LHS of the subquery is not yet supported (e.g. a non-attribute, non-foldable expression like {@code abs(x)}). In
     * that case we report the whole filter source (the entire {@code WHERE} clause).
     * <p>
     * Otherwise (the IN subquery is nested inside an expression that is not in the supported allowlist), we report the immediately
     * enclosing expression.
     */
    private static void checkInFilterCondition(
        Filter filter,
        Expression expr,
        boolean rewriteCurrentInSubquery,
        boolean insideRewriteBoundary,
        Expression outerExpr,
        Failures failures
    ) {
        if (expr instanceof InSubquery in) {
            if (rewriteCurrentInSubquery && insideRewriteBoundary == false) {
                failures.add(fail(in, "Complicated IN subquery is not yet supported in the WHERE command [{}]", filter.sourceText()));
            } else {
                failures.add(fail(in, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
            return;
        }
        if (expr instanceof MultiColumnInSubquery mcs) {
            if (rewriteCurrentInSubquery && insideRewriteBoundary == false) {
                failures.add(fail(mcs, "Complicated IN subquery is not yet supported in the WHERE command [{}]", filter.sourceText()));
            } else {
                failures.add(fail(mcs, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
            return;
        }

        boolean newInsideRewriteBoundary = insideRewriteBoundary || isInSubqueryRewriteBoundary(expr);
        boolean rewriteChildren = newInsideRewriteBoundary == false && canRewriteInSubqueryChildren(expr);
        Expression newOuterExpr = rewriteChildren ? null : newInsideRewriteBoundary && outerExpr != null ? outerExpr : expr;
        for (Expression child : expr.children()) {
            checkInFilterCondition(filter, child, rewriteChildren, newInsideRewriteBoundary, newOuterExpr, failures);
        }
    }
}
