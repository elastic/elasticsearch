/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAllFromDisjunctiveInSubquery;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Resolves {@link InSubquery} expressions in {@link Filter} conditions by rewriting them into
 * {@link SemiJoin} or {@link AntiJoin} nodes.
 * <p>
 * This runs before {@link PreAnalyzer} so that the subquery plans, which were embedded inside
 * {@link InSubquery} expressions, become children of join nodes and are visible to standard plan
 * tree traversals. This eliminates the need for separate InSubquery-aware traversals in
 * {@link PreAnalyzer}, {@link org.elasticsearch.xpack.esql.session.FieldNameUtils FieldNameUtils},
 * and {@link org.elasticsearch.xpack.esql.inference.InferenceResolver InferenceResolver}.
 * <p>
 * The join's rightFields are left empty at this stage because the subquery output is not yet resolved.
 * The Analyzer's {@code ResolveRefs} fills them in during the Resolution batch.
 */
public class InSubqueryResolver {

    /**
     * Resolves all {@link InSubquery} expressions in {@link Filter} conditions by rewriting them
     * into {@link SemiJoin} or {@link AntiJoin} nodes. After resolution, validates that no
     * {@link InSubquery} expressions remain in the plan. Any remaining expressions indicate
     * unsupported usage (e.g. in EVAL, SORT, STATS BY) and cause a {@link VerificationException}.
     *
     * @param listener receives the resolved plan on success, or a {@link VerificationException} on failure
     */
    public static void resolve(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        try {
            LogicalPlan resolved = resolveInSubqueries(plan);
            verify(resolved);
            listener.onResponse(resolved);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Synchronous variant for use in tests that don't need the listener pattern.
     * Throws {@link VerificationException} directly if unresolved {@link InSubquery} expressions remain.
     */
    public static LogicalPlan resolve(LogicalPlan plan) {
        LogicalPlan resolved = resolveInSubqueries(plan);
        verify(resolved);
        return resolved;
    }

    private static LogicalPlan resolveInSubqueries(LogicalPlan plan) {
        return plan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
    }

    private static LogicalPlan resolveInSubqueryInFilter(Filter filter) {
        Expression condition = filter.condition();

        // Handle OR disjuncts containing IN subqueries by rewriting to exclusive UnionAll branches.
        // Each branch gets AND-conjunct conditions that the existing SemiJoin extraction can handle.
        List<Expression> disjuncts = Predicates.splitOr(condition);
        if (disjuncts.size() > 1 && disjuncts.stream().anyMatch(InSubqueryResolver::containsInSubquery)) {
            return rewriteDisjunctiveInSubquery(filter, disjuncts);
        }

        List<Expression> conjuncts = Predicates.splitAnd(condition);

        List<Expression> remaining = new ArrayList<>();
        // Collect subquery joins to be placed on top
        record SemiOrAntiJoin(Source source, LogicalPlan subquery, JoinConfig config, boolean anti) {}
        List<SemiOrAntiJoin> semiOrAntiJoins = new ArrayList<>();
        // Synthetic Eval aliases for constant left-hand side expressions (e.g. WHERE 10001 IN (subquery))
        List<Alias> syntheticEvals = new ArrayList<>();

        for (Expression conjunct : conjuncts) {
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
                    // Constant expression: wrap in a synthetic Eval so it can be used as a join key.
                    // The synthetic Eval and its attribute are removed after the join by a Project.
                    var syntheticAlias = new Alias(
                        leftValue.source(),
                        syntheticName(leftValue, inSubquery.subquery()),
                        leftValue,
                        null,
                        true
                    );
                    syntheticEvals.add(syntheticAlias);
                    leftFields = singletonList(syntheticAlias.toAttribute());
                } else {
                    remaining.add(conjunct);
                    continue;
                }

                // Recursively resolve any nested IN subqueries within the subquery plan
                LogicalPlan subquery = resolveNestedInSubqueries(inSubquery.subquery());

                if (negated) {
                    JoinConfig config = new JoinConfig(JoinTypes.ANTI, leftFields, emptyList(), null);
                    semiOrAntiJoins.add(new SemiOrAntiJoin(inSubquery.source(), subquery, config, true));
                } else {
                    JoinConfig config = new JoinConfig(JoinTypes.SEMI, leftFields, emptyList(), null);
                    semiOrAntiJoins.add(new SemiOrAntiJoin(inSubquery.source(), subquery, config, false));
                }
            } else {
                remaining.add(conjunct);
            }
        }

        if (semiOrAntiJoins.isEmpty()) {
            return filter;
        }

        // Build the left side: remaining filter conditions on top of the original child, or just the child
        LogicalPlan current = remaining.isEmpty()
            ? filter.child()
            : new Filter(filter.source(), filter.child(), Predicates.combineAnd(remaining));

        // If any constants need materialization, insert an Eval to create the synthetic attributes
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(filter.source(), current, syntheticEvals);
        }

        // Stack SemiJoin/AntiJoin on top
        for (SemiOrAntiJoin sj : semiOrAntiJoins) {
            current = sj.anti
                ? new AntiJoin(sj.source, current, sj.subquery, sj.config)
                : new SemiJoin(sj.source, current, sj.subquery, sj.config);
        }

        return current;
    }

    /**
     * Recursively transforms a subquery plan, converting any nested IN/NOT IN subquery expressions
     * into SemiJoin/AntiJoin nodes. This is needed because nested subquery plans are embedded inside
     * InSubquery expressions and not reachable by the top-level transformUp.
     */
    private static LogicalPlan resolveNestedInSubqueries(LogicalPlan subqueryPlan) {
        return subqueryPlan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
    }

    /**
     * Rewrites a Filter with OR disjuncts containing InSubquery into an exclusive UnionAll.
     * For {@code WHERE d1 OR d2 OR d3}, produces:
     * <pre>
     * UnionAll(
     *   Filter(d1, child),
     *   Filter(NOT(d1) AND d2, child),
     *   Filter(NOT(d1) AND NOT(d2) AND d3, child)
     * )
     * </pre>
     * Each branch has only AND-conjunct conditions, which the existing SemiJoin extraction handles.
     * The exclusive partitioning ensures no duplicate rows across branches.
     */
    private static LogicalPlan rewriteDisjunctiveInSubquery(Filter filter, List<Expression> disjuncts) {
        LogicalPlan child = filter.child();
        Source source = filter.source();

        // Reorder disjuncts by complexity so that the exclusive-partitioning exclusions (NOT(d_i))
        // only negate expressions the resolver can handle:
        // 0 — no InSubquery at all (simplest, negation is trivial)
        // 1 — bare InSubquery or NOT(InSubquery) (negation flips IN/NOT IN, still handleable)
        // 2 — composite AND containing InSubquery (negation produces NOT(AND(...)) which the
        // resolver cannot decompose; placed last so their negation is never needed)
        List<Expression> reordered = new ArrayList<>(disjuncts);
        reordered.sort((a, b) -> Integer.compare(disjunctComplexity(a), disjunctComplexity(b)));

        List<LogicalPlan> branches = new ArrayList<>();
        List<Expression> exclusions = new ArrayList<>();

        for (Expression disjunct : reordered) {
            Expression branchCondition;
            if (exclusions.isEmpty()) {
                branchCondition = disjunct;
            } else {
                List<Expression> parts = new ArrayList<>();
                for (Expression ex : exclusions) {
                    parts.add(negate(ex));
                }
                parts.add(disjunct);
                branchCondition = Predicates.combineAnd(parts);
            }
            // Each branch's Filter may contain InSubquery in AND position — resolve them now
            LogicalPlan branch = resolveInSubqueryInFilter(new Filter(source, child, branchCondition));
            branches.add(branch);
            exclusions.add(disjunct);
        }

        return new UnionAllFromDisjunctiveInSubquery(source, branches, List.of());
    }

    /**
     * Returns a complexity score for ordering disjuncts in the exclusive query rewrite:
     * <ul>
     *   <li>0 — no InSubquery (plain predicate, not expensive to negate)</li>
     *   <li>1 — contains InSubquery but is not a composite AND (bare InSubquery or NOT(InSubquery), not expensive to negate)</li>
     *   <li>2 — composite AND containing InSubquery (NOT(AND(...)) are the last)</li>
     * </ul>
     */
    private static int disjunctComplexity(Expression expr) {
        if (containsInSubquery(expr) == false) {
            return 0;
        }
        return Predicates.splitAnd(expr).size() > 1 ? 2 : 1;
    }

    /**
     * Negates an expression, simplifying double negation: NOT(NOT(x)) → x.
     */
    private static Expression negate(Expression expr) {
        if (expr instanceof Not not) {
            return not.field();
        }
        return new Not(expr.source(), expr);
    }

    /**
     * Generates a unique synthetic name for the constant on the left-hand side of IN subquery.
     */
    private static String syntheticName(Expression value, LogicalPlan subquery) {
        return "$$in_subquery_const$" + value.hashCode() + "$" + subquery.hashCode();
    }

    private static boolean containsInSubquery(Expression expr) {
        if (expr instanceof InSubquery) {
            return true;
        }
        if (expr instanceof Not not) {
            return containsInSubquery(not.field());
        }
        return expr.children().stream().anyMatch(InSubqueryResolver::containsInSubquery);
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
     * {@link InSubqueryResolver} could not rewrite into a {@link SemiJoin}/{@link AntiJoin}.
     * <p>
     * If the IN subquery sits at the top of the boolean condition (i.e. only {@link And} /
     * {@link Or} / {@link Not} above it) the resolver normally rewrites it; if one survives here
     * it means the surrounding boolean shape is not yet supported (e.g. an IN subquery hidden
     * inside an unresolvable {@code OR} branch). In that case we report the whole filter source
     * (the entire {@code WHERE} clause) so the user can see the unsupported predicate.
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
