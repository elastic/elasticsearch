/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;

import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * Common validations to be used in {@link org.elasticsearch.xpack.esql.capabilities.Validatable} implementations.
 */
public final class Validations {

    private Validations() {}

    /**
     * Validates if the given expression is foldable - if not returns a Failure.
     */
    public static Failure isFoldable(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        if (e.foldable() == false) {
            return Failure.fail(
                e,
                format(
                    null,
                    "{}argument of [{}] must be a constant, received [{}]",
                    paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                    operationName,
                    Expressions.name(e)
                )
            );
        }

        return null;
    }

    /**
     * Validates if the given expression is foldable - if not returns a Failure.
     * If it is foldable, it calls the given validateFolded function with the folded value and a failure builder, to validate it.
     *
     * @param e the expression to validate
     * @param operationName the name of the operation
     * @param paramOrd the ordinal of the parameter
     * @param validateFolded the function to validate the folded value.
     *                       It receives the folded valua, and a function to generate the Failer based on a message.
     *                       It must return a Failure, or null if none
     */
    public static Failure isFoldableAnd(
        Expression e,
        String operationName,
        TypeResolutions.ParamOrdinal paramOrd,
        BiFunction<Object, Function<String, Failure>, Failure> validateFolded
    ) {
        var failure = isFoldable(e, operationName, paramOrd);

        if (failure == null) {
            failure = validateFolded.apply(
                e.fold(),
                message -> Failure.fail(
                    e,
                    format(
                        null,
                        "{}argument of [{}] {}, received [{}]",
                        paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                        operationName,
                        message,
                        Expressions.name(e)
                    )
                )
            );
        }

        return failure;
    }
}
