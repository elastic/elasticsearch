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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Resolves {@link InSubquery} expressions in {@link Filter} conditions and {@link Eval} field definitions
 * by rewriting them into {@link SemiJoin}, {@link AntiJoin}, or {@link MarkJoin} nodes:
 * <ul>
 *   <li>In a {@link Filter}: an {@code InSubquery} (optionally wrapped in {@link Not}) at the top of an
 *       AND-conjunct becomes a row-filtering {@link SemiJoin} / {@link AntiJoin} stacked on top of the
 *       remaining filter — the most efficient shape, used for the common conjunctive case. An
 *       {@code InSubquery} reachable through {@link Or}, {@link IsNull}/{@link IsNotNull}, or a
 *       {@code CASE}/{@code COALESCE} call is replaced with a synthetic boolean mark attribute and a
 *       {@link MarkJoin} stacked below the rewritten {@link Filter}.</li>
 *   <li>In an {@link Eval}: only {@link MarkJoin} is ever created — EVAL preserves every row and
 *       produces a value, so the row-filtering {@link SemiJoin}/{@link AntiJoin} shape is never
 *       applicable. The rewrite allowlist is the same as for {@link Filter}: bare {@code InSubquery},
 *       {@link And}/{@link Or}/{@link Not}, {@link IsNull}/{@link IsNotNull}, and {@code CASE}/
 *       {@code COALESCE}. {@code InSubquery} wrapped in any other expression is left in place for
 *       {@link #verify} to reject.</li>
 *   <li>An {@code InSubquery} wrapped in any other expression, or inside SORT / STATS BY / etc., is
 *       left in place; the post-resolution {@link #verify} step rejects the query with a
 *       {@link VerificationException}.</li>
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
     * Resolves all {@link InSubquery} expressions in {@link Filter} conditions and {@link Eval} field
     * definitions, and validates the result. Throws a {@link VerificationException} when an
     * {@link InSubquery} survived rewriting (e.g. inside a SORT, STATS BY clause, or wrapped in a
     * non-boolean expression).
     * <p>
     * Synchronous — does no I/O. Async callers should invoke this inside an
     * {@link org.elasticsearch.action.ActionListener#delegateFailureAndWrap delegateFailureAndWrap}
     * lambda so the thrown {@link VerificationException} is routed to {@code onFailure}.
     * <p>
     * Telemetry for {@code IN_SUBQUERY} is collected separately by the session — see
     * {@code EsqlSession#gatherInSubqueryMetrics}, which uses {@link #hasInSubquery} on
     * the pre-resolution plan because by the time this method returns the originating
     * {@link InSubquery} expressions have been replaced with
     * {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin} and are no longer visible to plan
     * traversals.
     */
    public static LogicalPlan resolve(LogicalPlan plan) {
        LogicalPlan resolved = resolveInSubqueries(plan);
        verify(resolved);
        return resolved;
    }

    private static LogicalPlan resolveInSubqueries(LogicalPlan plan) {
        return plan.transformUp(LogicalPlan.class, InSubqueryResolver::resolveInSubquery);
    }

    /**
     * Routes a plan node to the appropriate IN subquery resolver:
     * {@link #resolveInSubqueryInFilter} for {@link Filter}, {@link #resolveInSubqueryInEval} for
     * {@link Eval}.
     */
    private static LogicalPlan resolveInSubquery(LogicalPlan plan) {
        if (plan instanceof Filter filter) {
            return resolveInSubqueryInFilter(filter);
        }
        if (plan instanceof Eval eval) {
            return resolveInSubqueryInEval(eval);
        }
        return plan;
    }

    /**
     * Returns {@code true} if the pre-resolution plan contains any InSubquery expression anywhere in its expression trees. Used by
     * the session and {@link org.elasticsearch.xpack.esql.view.ViewResolver ViewResolver} to decide whether to run the resolution pass
     * and whether to increment the {@code IN_SUBQUERY} telemetry counter.
     * <p>
     * Conservatively checks all expressions in all plan nodes; unsupported positions (SORT, STATS BY) produce no rewrite in the resolver
     * but are then rejected by {@link #verify}.
     */
    public static boolean hasInSubquery(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p.expressions().stream().anyMatch(e -> e.anyMatch(x -> x instanceof InSubquery || x instanceof MultiColumnInSubquery))
        );
    }

    /**
     * Spec for a {@link SemiJoin} / {@link AntiJoin} stacked on top of the remaining filter for
     * an {@link InSubquery} that appears as a top-level AND conjunct.
     */
    private record SemiOrAntiJoinSpec(Source source, LogicalPlan subquery, JoinConfig config, boolean anti) {}

    /**
     * Spec for a {@link MarkJoin} stacked below the remaining filter (or rewritten EVAL) for an
     * {@link InSubquery} that appears under {@code OR}/{@code NOT}/{@code AND} but not as a top-level
     * AND conjunct, or anywhere inside an {@link Eval} field definition. The mark attribute is
     * referenced from the rewritten boolean expression.
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
            current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
        }

        // Apply remaining filter conditions on top of MarkJoins (so mark attributes are in scope).
        if (remaining.isEmpty() == false) {
            current = new Filter(filter.source(), current, Predicates.combineAnd(remaining));
        }

        // Stack SemiJoins / AntiJoins on top — they filter rows but don't modify columns.
        for (SemiOrAntiJoinSpec sj : semiOrAntiJoins) {
            current = sj.anti()
                ? new AntiJoin(sj.source(), current, sj.subquery(), sj.config())
                : new SemiJoin(sj.source(), current, sj.subquery(), sj.config());
        }

        // The mark attributes from MarkJoins (and any synthetic constant Eval columns introduced
        // for foldable LHS) are flagged synthetic so the analyzer's default output projection
        // (planWithoutSyntheticAttributes) drops them — preserving the filter's apparent schema.
        return current;
    }

    /**
     * Resolves {@link InSubquery} expressions inside {@link Eval} field definitions by replacing them with synthetic mark attributes
     * and stacking {@link MarkJoin} nodes below the rewritten {@link Eval}. Returns the identical {@code eval} instance when no
     * {@link InSubquery} is found.
     * <p>
     * Unlike {@link #resolveInSubqueryInFilter}, only {@link MarkJoin} is ever created here — EVAL must preserve every row and produce a
     * value, so the row-filtering {@link SemiJoin}/{@link AntiJoin} shape is never applicable.
     * <p>
     * When an IN subquery's left-hand side is a field produced by an earlier alias in the same EVAL
     * (e.g. {@code EVAL a = emp_no + 1, b = a IN (sub)}), this method splits the {@link Eval}: the preceding aliases are flushed into a
     * lower {@link Eval} so the LHS field is in scope for the {@link MarkJoin}. When no such dependency exists, a single {@link MarkJoin}
     * is stacked below the whole {@link Eval} regardless of how many aliases it defines.
     * <p>
     * Make this public, so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive IN subquery resolution.
     */
    public static LogicalPlan resolveInSubqueryInEval(Eval eval) {
        // LinkedHashMap preserves insertion order (needed when flushing into an Eval) and exposes
        // its keySet() as the name-lookup set — a single structure instead of a parallel List+Set pair.
        LinkedHashMap<String, Alias> pending = new LinkedHashMap<>();
        LogicalPlan current = eval.child();
        boolean changed = false;

        // Shared accumulators grow monotonically across all fields so that the ordinal passed to
        // syntheticMarkName / syntheticConstName (joins.size() / syntheticEvals.size() at call time)
        // is globally unique. Per-field slices are taken after each rewriteInSubqueries call.
        List<MarkJoinSpec> allMarks = new ArrayList<>();
        List<Alias> allConsts = new ArrayList<>();

        for (Alias field : eval.fields()) {
            int marksBefore = allMarks.size();
            int constsBefore = allConsts.size();
            Expression rewritten = rewriteInSubqueries(field.child(), true, allMarks, allConsts);
            List<MarkJoinSpec> fieldMarks = new ArrayList<>(allMarks.subList(marksBefore, allMarks.size()));
            List<Alias> fieldConsts = new ArrayList<>(allConsts.subList(constsBefore, allConsts.size()));

            if (fieldMarks.isEmpty()) {
                // No InSubquery in this field — accumulate as-is.
                pending.put(field.name(), field);
                continue;
            }

            changed = true;

            // Flush accumulated fields below the MarkJoin(s) only when a mark's left field references an alias produced by a preceding
            // field. This makes the LHS attribute available below the join, e.g. for: EVAL a = emp_no + 1, b = a IN (sub), without the
            // flush, `a` would not exist below the MarkJoin.
            if (pending.isEmpty() == false && referencesPendingName(fieldMarks, pending.keySet())) {
                current = new Eval(eval.source(), current, new ArrayList<>(pending.values()));
                pending = new LinkedHashMap<>();
            }

            // Materialize any foldable LHS constants (e.g. EVAL b = 10001 IN (sub)).
            if (fieldConsts.isEmpty() == false) {
                current = new Eval(eval.source(), current, fieldConsts);
            }

            // Stack MarkJoins so the mark attributes are available to the rewritten EVAL field.
            for (MarkJoinSpec mj : fieldMarks) {
                current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
            }

            // Re-create the alias with the rewritten expression, preserving the original NameId so
            // downstream references (e.g. a later WHERE m) resolve to the right column.
            pending.put(field.name(), new Alias(field.source(), field.name(), rewritten, field.id(), field.synthetic()));
        }

        if (changed == false) {
            return eval; // nothing changed — return the identical instance for reference-equality checks
        }

        if (pending.isEmpty() == false) {
            current = new Eval(eval.source(), current, new ArrayList<>(pending.values()));
        }
        return current;
    }

    /**
     * Returns {@code true} if any mark join's left field shares a name with one of the {@code pendingNames} — indicating that the join's
     * LHS depends on an alias produced by a pending (preceding) EVAL field that must be flushed below the join first.
     * <p>
     * Only {@code leftFields()} are checked, not {@code fieldConsts} alias names. {@code fieldConsts} aliases are generated by
     * {@link #syntheticConstName}, which always produces names of the form {@code $$in_subquery_const$...}. These are structurally
     * disjoint from any user-defined EVAL alias name (which cannot start with {@code $$}), so they can never appear in
     * {@code pendingNames} and checking them would always yield {@code false}.
     */
    private static boolean referencesPendingName(List<MarkJoinSpec> fieldMarks, Set<String> pendingNames) {
        for (MarkJoinSpec mj : fieldMarks) {
            for (Attribute leftField : mj.config().leftFields()) {
                if (pendingNames.contains(leftField.name())) {
                    return true;
                }
            }
        }
        return false;
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
        // resolveInSubqueries is reused here: nested subquery plans are not reachable by the
        // outer transformUp (they live inside InSubquery expressions, not as plan children), so
        // they need their own traversal — but the traversal strategy is identical to the outer one.
        LogicalPlan subquery = resolveInSubqueries(subqueryPlan);
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
        // resolveInSubqueries is reused here: nested subquery plans are not reachable by the
        // outer transformUp (they live inside InSubquery expressions, not as plan children), so
        // they need their own traversal — but the traversal strategy is identical to the outer one.
        LogicalPlan subquery = resolveInSubqueries(inSubquery.subquery());
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
        // resolveInSubqueries is reused here: see the single-column overload for the rationale.
        LogicalPlan subquery = resolveInSubqueries(mcs.subquery());
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
     * TODO: consider a deduplication optimization as a future enhancement, so that {@code (1, 1) IN (subquery) OR (1, 1) IN (subquery)}
     *  produces only one join and one synthetic alias.
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
        plan.forEachDown(p -> {
            if (p instanceof Filter filter) {
                checkInSubqueryExpression(filter, filter.condition(), true, false, null, failures);
            } else if (p instanceof Eval eval) {
                for (Alias field : eval.fields()) {
                    checkInSubqueryExpression(eval, field.child(), true, false, null, failures);
                }
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
     * Walks the {@code WHERE} condition or an {@code Eval} field expression tree to validate IN subquery usage that the
     * {@code InSubqueryResolver} could not rewrite into a {@code SemiJoin}/{@code AntiJoin}/{@code MarkJoin}.
     * <p>
     * If the IN subquery is directly below an eligible boolean-composing expression, the resolver normally rewrites it; if one survives
     * here it means the LHS of the subquery is not yet supported (e.g. a non-attribute, non-foldable expression like {@code abs(x)}). In
     * that case we report the whole filter source (the entire {@code WHERE} clause).
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
                failures.add(fail(expr, "IN subquery is not supported within expression [{}]", outerExpr.sourceText()));
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
