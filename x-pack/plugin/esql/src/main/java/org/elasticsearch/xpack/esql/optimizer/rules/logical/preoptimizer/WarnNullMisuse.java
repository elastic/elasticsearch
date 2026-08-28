/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NullMisuseSuggestion;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Warns about expressions that always evaluate to {@code NULL} because the user explicitly wrote a
 * {@code NULL} literal in a position where nulls propagate virally, e.g. {@code x == NULL} or
 * {@code a + NULL}. Comparisons additionally suggest the {@code IS NULL} / {@code IS NOT NULL}
 * spelling via {@link NullMisuseSuggestion}.
 * <p>
 * The warning points at the innermost expression having the {@code NULL} literal as a direct child,
 * so the user sees the exact spot of the problem instead of some enclosing expression the null
 * propagated to. The one exception is {@code !=}, which is parsed as {@code NOT(==)}: the warning
 * targets the {@code NOT} (both share the source text the user wrote) so the suggestion can be
 * {@code IS NOT NULL}.
 * <p>
 * This runs as a pre-optimizer step, on the analyzed plan, on purpose:
 * <ul>
 *     <li>It runs once, and only on the coordinator, so data node re-optimization can't re-emit warnings.</li>
 *     <li>Optimizer rewrites (surrogates, constant propagation, fields nullified by the local optimizer)
 *     haven't happened yet, so warnings always point at something the user actually wrote.</li>
 * </ul>
 * Only explicit {@code NULL} literals count as null sources: null-typed attributes (e.g. unmapped fields
 * nullified by {@code SET unmapped_fields = "nullify"}, or references to a null {@code EVAL} alias) and
 * nulls synthesized by lowerings such as PROMQL carry a different source text and are ignored.
 */
public class WarnNullMisuse implements LogicalPlanPreOptimizerRule {

    @Override
    public void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        ActionListener.completeWith(listener, () -> {
            plan.forEachDown(node -> node.expressions().forEach(WarnNullMisuse::check));
            return plan;
        });
    }

    private static void check(Expression e) {
        // `!=` is parsed as NOT(==) sharing the same source; warn on the NOT so the message matches
        // what the user wrote and the suggestion is IS NOT NULL rather than IS NULL.
        if (e instanceof Not not && isNullComparison(not.field())) {
            warnNullLiteral(not);
            not.field().children().forEach(WarnNullMisuse::check);
            return;
        }
        if (FoldNull.foldsToNull(e, WarnNullMisuse::isExplicitNullLiteral)) {
            warnNullLiteral(e);
        }
        // Keep descending: other children may misuse their own, different NULL literal.
        e.children().forEach(WarnNullMisuse::check);
    }

    private static boolean isNullComparison(Expression e) {
        return (e instanceof Equals || e instanceof InsensitiveEquals) && FoldNull.foldsToNull(e, WarnNullMisuse::isExplicitNullLiteral);
    }

    private static boolean isExplicitNullLiteral(Expression e) {
        return e instanceof Literal literal && literal.value() == null && literal.sourceText().equalsIgnoreCase("null");
    }

    private static void warnNullLiteral(Expression e) {
        // Synthetic expressions (e.g. Source.EMPTY) have no valid location to point the user at.
        if (e.sourceLocation().getLineNumber() < 0) {
            return;
        }
        String alternative = e instanceof NullMisuseSuggestion suggestion ? suggestion.nullMisuseAlternative() : null;
        if (alternative != null) {
            addWarning(
                "Line {}:{}: Expression [{}] always evaluates to NULL, did you mean [{}]?",
                e.sourceLocation().getLineNumber(),
                e.sourceLocation().getColumnNumber(),
                e.sourceText(),
                alternative
            );
        } else {
            addWarning(
                "Line {}:{}: Expression [{}] always evaluates to NULL.",
                e.sourceLocation().getLineNumber(),
                e.sourceLocation().getColumnNumber(),
                e.sourceText()
            );
        }
    }
}
