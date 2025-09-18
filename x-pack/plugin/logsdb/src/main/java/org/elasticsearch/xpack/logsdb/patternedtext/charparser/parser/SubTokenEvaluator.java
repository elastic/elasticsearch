/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Provides functionality for the evaluation of a sub-tokens based on a given {@code CharSequence} input.
 * Evaluation means the computation of a numeric value based on the input, which serves for two purposes:
 * <ol>
 *     <li>A non-negative value indicates that the input matches the criteria defined by the evaluator, thus indicates that the
 *     sub-token bitmask that is associated with the evaluator is valid for this input.</li>
 *     <li>Wherever applicable, the computed value can be used as the numeric representation of the sub-token, such as in the case of
 *     {@link CharSequence} sub-tokens that map to a numeric value.</li>
 * </ol>
 * Compound {@link SubTokenEvaluator} can be generated using logical AND and OR operations.
 *
 * @param <T> the type of the input, which must extend CharSequence
 */
public final class SubTokenEvaluator<T extends CharSequence> {

    public final int bitmask;
    public final ToIntFunction<T> toIntFunction;

    public SubTokenEvaluator(int bitmask, ToIntFunction<T> toIntFunction) {
        this.bitmask = bitmask;
        this.toIntFunction = toIntFunction;
    }

    /**
     * Evaluates a numeric value based on the provided input. A non-negative value indicates that the input matches the criteria, which
     * means that the sub-token bitmask associated with this evaluator is valid for the input. The numeric value also serves as the numeric
     * representation of the sub-token, where applicable (e.g., the month October is represented by value 10).
     * @param input the input to evaluate
     * @return an integer value representing the provided input
     */
    public int evaluate(final T input) {
        // todo - this is called for very limited tokens, only such that have a non-zero bitmask, therefore it makes sense to cache
        // the result statically
        return toIntFunction.applyAsInt(input);
    }

    /**
     * The AND operation for combining two {@link SubTokenEvaluator} instances should be associated with the bitwise AND operation on their
     * bitmasks and evaluate negative values for inputs for which either of the evaluators returns a negative value.
     * @param other the other SubTokenEvaluator to combine with this one
     * @return a new SubTokenEvaluator that combines the two evaluators using the AND operation
     */
    public SubTokenEvaluator<T> and(SubTokenEvaluator<T> other) {
        Objects.requireNonNull(other);
        return new SubTokenEvaluator<>(this.bitmask & other.bitmask, input -> {
            int thisEval = this.toIntFunction.applyAsInt(input);
            int otherEval = other.toIntFunction.applyAsInt(input);
            if (thisEval < 0 || otherEval < 0) {
                // if any of the evaluators return a negative value, we return the minimum of the two evaluations
                return Math.min(thisEval, otherEval);
            } else {
                // otherwise, we return the maximum of the two evaluations
                // todo - is this the right logic, or should we throw an exception if values are unequal?
                return Math.max(thisEval, otherEval);
            }
        });
    }

    /**
     * The OR operation for combining two {@link SubTokenEvaluator} instances should be associated with the bitwise OR operation on their
     * bitmasks and evaluate negative values for inputs for which both of the evaluators return a negative value.
     * @param other the other SubTokenEvaluator to combine with this one
     * @return a new SubTokenEvaluator that combines the two evaluators using the OR operation
     */
    public SubTokenEvaluator<T> or(SubTokenEvaluator<T> other) {
        Objects.requireNonNull(other);
        return new SubTokenEvaluator<>(this.bitmask | other.bitmask, input -> {
            int thisEval = this.toIntFunction.applyAsInt(input);
            int otherEval = other.toIntFunction.applyAsInt(input);
            if (thisEval < 0 && otherEval < 0) {
                // if both evaluators return a negative value, we return the minimum of the two evaluations
                return Math.min(thisEval, otherEval);
            } else {
                // otherwise, we return the maximum of the two evaluations
                // todo - is this the right logic, or should we throw an exception if values are unequal?
                return Math.max(thisEval, otherEval);
            }
        });
    }
}
