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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Warns when an explicit, user-written {@code NULL} literal makes an expression always
 * {@code NULL} (e.g. {@code x == NULL}, {@code a + NULL}). Runs once on the analyzed plan,
 * coordinator-only, so later optimizer rewrites cannot invent extra warnings.
 * <p>
 * Special cases:
 * <ul>
 *     <li>{@code IN} — a {@code NULL} in the list is ignored, not treated as always-null.</li>
 *     <li>{@code !=} and {@code NOT IN} — warn on the parsed {@code NOT} so the message
 *     matches the source the user wrote.</li>
 * </ul>
 * Only literals whose source text is {@code NULL} count; null-typed attributes and
 * synthesized nulls do not.
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
        // `NOT IN` is parsed as NOT(IN) sharing the user's source; warn on the NOT.
        if (e instanceof Not not && not.field() instanceof In in && hasExplicitNullInList(in)) {
            warnInListNull(not, in, true);
            in.children().forEach(WarnNullMisuse::check);
            return;
        }
        // `!=` is parsed as NOT(==) sharing the same source; warn on the NOT so the suggestion is IS NOT NULL.
        if (e instanceof Not not && isNullComparison(not.field())) {
            warnNullLiteral(not);
            not.field().children().forEach(WarnNullMisuse::check);
            return;
        }
        if (e instanceof In in && hasExplicitNullInList(in)) {
            warnInListNull(in, in, false);
            in.children().forEach(WarnNullMisuse::check);
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

    private static boolean hasExplicitNullInList(In in) {
        return in.list().stream().anyMatch(WarnNullMisuse::isExplicitNullLiteral);
    }

    private static boolean isExplicitNullLiteral(Expression e) {
        return e instanceof Literal literal && literal.value() == null && literal.sourceText().equalsIgnoreCase("null");
    }

    /**
     * {@code NULL} in an {@code IN} list never matches. Do not try to rewrite the {@code IN} source
     * without the nulls: the text the user wrote may not even be an {@code IN} after parsing.
     */
    private static void warnInListNull(Expression source, In in, boolean negated) {
        if (source.sourceLocation().getLineNumber() < 0) {
            return;
        }
        String kept = operandText(in.value());
        boolean allNull = in.list().stream().allMatch(WarnNullMisuse::isExplicitNullLiteral);
        if (allNull && kept != null) {
            addWarning(
                "Line {}:{}: NULL in the IN list of [{}] is ignored, did you mean [{}]?",
                source.sourceLocation().getLineNumber(),
                source.sourceLocation().getColumnNumber(),
                source.sourceText(),
                kept + (negated ? " IS NOT NULL" : " IS NULL")
            );
        } else if (allNull == false && negated == false && kept != null) {
            addWarning(
                "Line {}:{}: NULL in the IN list of [{}] is ignored, you can move it to [{}].",
                source.sourceLocation().getLineNumber(),
                source.sourceLocation().getColumnNumber(),
                source.sourceText(),
                "OR " + kept + " IS NULL"
            );
        } else {
            addWarning(
                "Line {}:{}: NULL in the IN list of [{}] is ignored.",
                source.sourceLocation().getLineNumber(),
                source.sourceLocation().getColumnNumber(),
                source.sourceText()
            );
        }
    }

    private static String operandText(Expression e) {
        if (e instanceof Literal) {
            return null;
        }
        String text = e.sourceText();
        return text.isEmpty() ? null : text;
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
