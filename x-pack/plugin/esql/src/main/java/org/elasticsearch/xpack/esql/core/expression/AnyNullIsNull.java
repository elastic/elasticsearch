/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Marker interface for expressions that propagate {@code null} through <b>all</b>
 * of their arguments: if any argument evaluates to {@code null} at a given position,
 * the expression itself evaluates to {@code null} at that position.
 * <p>
 * For aggregation functions, this means that if any argument is always
 * {@code null}, the aggregated value will also be {@code null}.
 * <p>
 * If a function has this property, it <b>must</b> be marked with this interface,
 * so that optimizer rules can apply.
 * <p>
 * Folding must also return {@code null} if any argument folds to {@code null}. Optimizer rules may fold these
 * expressions before {@code FoldNull} replaces them with {@code null}, with arguments that aren't {@code null}
 * literals yet (a {@code CASE} without a match, {@code 1/0}...), or are typed {@code null}s.
 * {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper#fold} handles this before calling
 * {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper#doFold}, so to customize folding, override
 * {@code doFold} instead of {@link Expression#fold}.
 */
public interface AnyNullIsNull {

    /**
     * Folds the foldable arguments of an {@link AnyNullIsNull} expression, so it can fold to {@code null} if any of
     * them is {@code null}, even if they aren't {@code null} literals yet (like {@code CASE(false, 1)} or {@code 1/0}).
     * Arguments that aren't foldable, like options maps, are kept as they are.
     *
     * @return {@code null} if any argument folds to {@code null};
     *         a copy of the expression with its foldable arguments replaced by their folded {@link Literal}s;
     *         or the expression itself if they were already literals.
     */
    static Expression foldArguments(Expression expression, FoldContext ctx) {
        List<Expression> arguments = expression.children();
        List<Expression> foldedArguments = new ArrayList<>(arguments.size());
        boolean changed = false;
        for (Expression argument : arguments) {
            if (argument.foldable() == false) {
                foldedArguments.add(argument);
                continue;
            }
            Literal folded = Literal.of(ctx, argument);
            if (folded.value() == null) {
                return null;
            }
            changed |= folded != argument;
            foldedArguments.add(folded);
        }
        return changed ? expression.replaceChildren(foldedArguments) : expression;
    }
}
