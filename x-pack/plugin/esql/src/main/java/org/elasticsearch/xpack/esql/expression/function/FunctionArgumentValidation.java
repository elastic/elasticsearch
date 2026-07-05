/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Validation for argument counts.
 */
class FunctionArgumentValidation {

    private FunctionArgumentValidation() {}

    // Translation table for error messaging in the following functions
    private static final String[] NUM_NAMES = { "zero", "one", "two", "three", "four", "five", "six" };

    static final BiConsumer<Source, List<Expression>> NO_ARGS = (source, children) -> {
        if (false == children.isEmpty()) {
            throw new QlIllegalArgumentException("expects no arguments");
        }
    };

    static final BiConsumer<Source, List<Expression>> UNARY = (source, children) -> {
        if (children.size() != 1) {
            throw new QlIllegalArgumentException("expects exactly one argument");
        }
    };

    static BiConsumer<Source, List<Expression>> binary(Class<? extends Function> function) {
        if (OptionalArgument.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.size() > 2 || children.isEmpty()) {
                    throw new QlIllegalArgumentException("expects one or two arguments");
                }
            };
        }
        return (source, children) -> {
            if (children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
        };
    }

    static BiConsumer<Source, List<Expression>> quinaryTs(Class<? extends Function> function, int numOptionalParams) {
        return quinaryWithTotal(function, 4, numOptionalParams);
    }

    static BiConsumer<Source, List<Expression>> quinary(Class<? extends Function> function, int numOptionalParams) {
        return quinaryWithTotal(function, 5, numOptionalParams);
    }

    private static BiConsumer<Source, List<Expression>> quinaryWithTotal(
        Class<? extends Function> function,
        int numTotalParams,
        int numOptionalParams
    ) {
        if (OptionalArgument.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.size() > numTotalParams || children.size() < numTotalParams - numOptionalParams) {
                    throw new QlIllegalArgumentException(
                        "expects between "
                            + NUM_NAMES[numTotalParams - numOptionalParams]
                            + " and "
                            + NUM_NAMES[numTotalParams]
                            + " arguments"
                    );
                }
            };
        }
        return (source, children) -> {
            if (children.size() != numTotalParams) {
                throw new QlIllegalArgumentException("expects exactly " + NUM_NAMES[numTotalParams] + " arguments");
            }
        };
    }

    static BiConsumer<Source, List<Expression>> unaryVariadic(Class<? extends Function> function) {
        if (OptionalArgument.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.isEmpty()) {
                    throw new QlIllegalArgumentException("expects at least one argument");
                }
            };
        }
        return (source, children) -> {
            if (children.size() < 2) {
                throw new QlIllegalArgumentException("expects at least two arguments");
            }
        };
    }

    static BiConsumer<Source, List<Expression>> ternary(Class<? extends Function> function) {
        if (TwoOptionalArguments.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.size() > 3 || children.isEmpty()) {
                    throw new QlIllegalArgumentException("expects one, two or three arguments");
                }
            };
        }
        if (OptionalArgument.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.size() > 3 || children.size() < 2) {
                    throw new QlIllegalArgumentException("expects two or three arguments");
                }
            };
        }
        return (source, children) -> {
            if (children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
        };
    }

    static BiConsumer<Source, List<Expression>> quaternary(Class<? extends Function> function) {
        if (OptionalArgument.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.size() > 4 || children.size() < 3) {
                    throw new QlIllegalArgumentException("expects three or four arguments");
                }
            };
        }
        if (TwoOptionalArguments.class.isAssignableFrom(function)) {
            return (source, children) -> {
                if (children.size() > 4 || children.size() < 2) {
                    throw new QlIllegalArgumentException("expects minimum two, maximum four arguments");
                }
            };
        }
        return (source, children) -> {
            if (children.size() != 4) {
                throw new QlIllegalArgumentException("expects exactly four arguments");
            }
        };
    }
}
