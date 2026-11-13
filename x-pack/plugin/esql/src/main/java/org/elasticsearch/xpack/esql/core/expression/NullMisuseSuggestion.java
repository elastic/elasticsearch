/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;

/**
 * Optional rewrite hint for the null misuse warning, emitted when an expression will always
 * evaluate to {@code NULL} because a child is an explicitly written {@code NULL}.
 * <p>
 * Most viral-null expressions have no better spelling and return {@code null}. Comparisons
 * that users often confuse with {@code IS NULL} implement this.
 */
public interface NullMisuseSuggestion {
    /**
     * Alternative the user may have meant, or {@code null} if there is no rewrite to suggest.
     */
    @Nullable
    String nullMisuseAlternative();

    /**
     * Rewrites an equality against {@code NULL}, which is always {@code NULL}, into the {@code IS NULL} check
     * the user most likely meant. Only meant for the operands of an equality operator ({@code ==}, {@code =~}):
     * <ul>
     *     <li>{@code x == NULL} or {@code NULL == x}: {@code x IS NULL}</li>
     *     <li>{@code NOT (x == NULL)}, which is how {@code x != NULL} is parsed, with {@code negated}:
     *     {@code x IS NOT NULL}</li>
     * </ul>
     * Returns {@code null} if neither operand is {@code NULL}, or there is nothing worth suggesting
     * (see {@link #isNullCheckText}).
     */
    @Nullable
    static String equalsNullToIsNull(Expression left, Expression right, boolean negated) {
        if (isNullOperand(right)) {
            return isNullCheckText(left, negated);
        }
        if (isNullOperand(left)) {
            return isNullCheckText(right, negated);
        }
        return null;
    }

    /**
     * Source text of {@code <operand> IS NULL}, or {@code <operand> IS NOT NULL} if {@code negated}.
     * Returns {@code null} if the operand is a literal (suggesting {@code 5 IS NULL} is never what the user
     * meant) or has no source text.
     */
    @Nullable
    static String isNullCheckText(Expression operand, boolean negated) {
        if (operand instanceof Literal) {
            return null;
        }
        String text = operand.sourceText();
        return text.isEmpty() ? null : text + (negated ? " IS NOT NULL" : " IS NULL");
    }

    /**
     * A {@code NULL} operand, including a cast or conversion of an explicit {@code NULL} ({@code null::integer}).
     */
    static boolean isNullOperand(Expression e) {
        return Expressions.isGuaranteedNull(e) || ConvertFunction.isExplicitNull(e);
    }
}
