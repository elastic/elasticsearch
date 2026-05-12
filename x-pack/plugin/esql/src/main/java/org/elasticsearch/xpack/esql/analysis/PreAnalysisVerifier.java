/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Validation that runs after the parser (and view resolution) but before the pre-analyzer
 * field-caps round trip and the rest of analysis.
 *
 * <p>Putting checks here means a query with a structural error fails immediately and
 * cheaply — without paying for index resolution / field-caps calls. The trade-off is that
 * we only see the parsed plan, so checks here must be expressible without resolved
 * attributes, types, or indices.
 *
 * <h2>{@link InSubquery} rules</h2>
 *
 * <ol>
 *   <li>It can only appear inside a top-level {@code WHERE} pipe (i.e., a {@link Filter}
 *       plan node). Per-aggregation {@code WHERE} filters in {@code STATS} /
 *       {@code INLINE STATS} look syntactically like a {@code WHERE} clause, but they're
 *       aggregation-level {@code FilteredExpression}s on {@link Aggregate} /
 *       {@link InlineStats}, not {@link Filter} plan nodes — so they get a tailored
 *       message that names the offending command. This rule is permanent — it stays
 *       after the feature is fully implemented.</li>
 *   <li>Within a {@code WHERE} pipe it must occupy a boolean predicate position: the
 *       condition itself, or an operand reachable only through {@link And} / {@link Or}
 *       / {@link Not}. Nesting it as an argument to a scalar function (e.g.
 *       {@code MV_CONTAINS(x IN (...), ...)}) treats the subquery as a value, which is
 *       not allowed. This rule is permanent.</li>
 *   <li>The feature is not yet supported, so even valid uses inside {@code WHERE} are
 *       rejected.
 *       TODO: remove this rule once the {@code InSubqueryResolver} / optimizer / executor
 *       PRs land.</li>
 * </ol>
 */
public final class PreAnalysisVerifier {

    private PreAnalysisVerifier() {}

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
                checkInFilterCondition(filter.condition(), null, failures);
            } else {
                p.forEachExpression(
                    InSubquery.class,
                    inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", p.sourceText()))
                );
            }
        });
    }

    /**
     * Walks the {@code WHERE} condition tree. If the IN subquery appears as a top expression
     * (for example as a child of {@link And} / {@link Or} / {@link Not}), we fail with a
     * message IN subquery is not yet supported (which will be removed once the
     * {@code InSubqueryResolver} / optimizer / executor PRs land). If we are in a
     * inner expression (i.e. children of anything else (notably scalar functions) we fail with
     * a message that reflects that)
     */
    private static void checkInFilterCondition(Expression expr, Expression outerExpr, Failures failures) {
        if (expr instanceof InSubquery in) {
            if (outerExpr == null) {
                failures.add(fail(in, "IN subquery is not yet supported"));
            } else {
                failures.add(fail(in, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
        }
        Expression newOuterExpr = outerExpr == null
            && expr instanceof And == false
            && expr instanceof Or == false
            && expr instanceof Not == false ? expr : outerExpr;
        for (Expression child : expr.children()) {
            checkInFilterCondition(child, newOuterExpr, failures);
        }
    }
}
