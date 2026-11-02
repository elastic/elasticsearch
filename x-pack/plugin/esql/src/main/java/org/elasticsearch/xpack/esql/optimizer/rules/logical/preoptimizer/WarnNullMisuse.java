/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NullMisuseSuggestion;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Warns when an explicit, user-written {@code NULL} literal makes an expression always
 * {@code NULL} (e.g. {@code x == NULL}, {@code a + NULL}). Runs once on the analyzed plan,
 * coordinator-only, so later optimizer rewrites cannot invent extra warnings.
 * <p>
 * Special cases:
 * <ul>
 *     <li>{@code IN} — a {@code NULL} in a list with other values never matches, but a match still yields
 *     {@code true}, so it gets its own warning. An all-{@code NULL} list is always {@code NULL}.</li>
 *     <li>{@code !=} and {@code NOT IN} — warn on the parsed {@code NOT} so the message
 *     matches the source the user wrote.</li>
 *     <li>A cast or conversion of an explicit {@code NULL} ({@code null::string}, {@code TO_INTEGER(NULL)})
 *     is a typed null, not a warning. A parent that uses it still warns.</li>
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
        // null::type and TO_TYPE(null) are how you spell a typed null. Skip the conversion itself;
        // parents still see it via isExplicitNull.
        if (e instanceof ConvertFunction && ConvertFunction.isExplicitNull(e)) {
            return;
        }
        // `NOT IN` is parsed as NOT(IN) sharing the user's source; warn on the NOT.
        if (e instanceof Not not && not.field() instanceof In in && hasExplicitNullInList(in)) {
            warnInWithNull(not, in, true);
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
            warnInWithNull(in, in, false);
            in.children().forEach(WarnNullMisuse::check);
            return;
        }
        if (foldsToNull(e)) {
            warnNullLiteral(e);
        }
        // Keep descending: other children may misuse their own, different NULL literal.
        e.children().forEach(WarnNullMisuse::check);
    }

    private static boolean isNullComparison(Expression e) {
        return (e instanceof Equals || e instanceof InsensitiveEquals) && foldsToNull(e);
    }

    private static boolean foldsToNull(Expression e) {
        if (e instanceof In in) {
            return ConvertFunction.isExplicitNull(in.value());
        }
        if (e instanceof FilteredExpression fe) {
            return ConvertFunction.isExplicitNull(fe.filter()) && fe.delegate().nullable() == Nullability.TRUE;
        }
        return e instanceof EvaluatorMapper
            && e instanceof AnyNullIsNull
            && e.children().stream().anyMatch(ConvertFunction::isExplicitNull);
    }

    private static boolean hasExplicitNullInList(In in) {
        return in.list().stream().anyMatch(ConvertFunction::isExplicitNull);
    }

    /**
     * A {@code NULL} probe or an all-{@code NULL} list makes the {@code IN} always {@code NULL}, like the
     * one-element {@code x IN (NULL)} the parser turns into {@code x == NULL}. Otherwise the list {@code NULL}
     * never matches. Do not try to rewrite the {@code IN} source without the nulls: the text the user wrote
     * may not even be an {@code IN} after parsing.
     */
    private static void warnInWithNull(Expression source, In in, boolean negated) {
        if (ConvertFunction.isExplicitNull(in.value()) || in.list().stream().allMatch(ConvertFunction::isExplicitNull)) {
            warnNullLiteral(source);
            return;
        }
        if (source.sourceLocation().getLineNumber() < 0) {
            return;
        }
        if (negated) {
            // No IS NULL check gives back what the user meant, so there is no hint.
            addWarning(
                "Line {}:{}: NULL in the IN list of [{}] makes the expression never true.",
                source.sourceLocation().getLineNumber(),
                source.sourceLocation().getColumnNumber(),
                source.sourceText()
            );
            return;
        }
        String isNull = NullMisuseSuggestion.isNullCheckText(in.value(), false);
        if (isNull != null) {
            addWarning(
                "Line {}:{}: NULL in the IN list of [{}] never matches, add [OR {}] to also match NULL values.",
                source.sourceLocation().getLineNumber(),
                source.sourceLocation().getColumnNumber(),
                source.sourceText(),
                isNull
            );
        } else {
            addWarning(
                "Line {}:{}: NULL in the IN list of [{}] never matches.",
                source.sourceLocation().getLineNumber(),
                source.sourceLocation().getColumnNumber(),
                source.sourceText()
            );
        }
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
