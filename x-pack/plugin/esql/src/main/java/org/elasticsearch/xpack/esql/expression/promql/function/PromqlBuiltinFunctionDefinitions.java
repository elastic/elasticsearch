/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;

/**
 * PromQL built-in function definitions that do not correspond to a dedicated ES|QL function class.
 */
class PromqlBuiltinFunctionDefinitions {

    static final PromqlFunctionDefinition VECTOR = PromqlFunctionDefinition.def()
        .vectorConversion()
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns the scalar as a vector with no labels.")
        .example("vector(1)")
        .name("vector");

    static final PromqlFunctionDefinition SCALAR = PromqlFunctionDefinition.def()
        .scalarConversion(Scalar::new)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            Returns the sample value of a single-element instant vector as a scalar. \
            If the input vector does not have exactly one element, scalar returns NaN.""")
        .example("scalar(sum(http_requests_total))")
        .name("scalar");

    static final PromqlFunctionDefinition TIME = PromqlFunctionDefinition.def()
        .scalarWithStep((source, step) -> new Div(source, new ToDouble(source, step), Literal.fromDouble(source, 1000.0)))
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("""
            returns the number of seconds since January 1, 1970 UTC. \
            Note that this does not actually return the current time, but the time at which the expression is to be evaluated.""")
        .example("time()")
        .name("time");

    private PromqlBuiltinFunctionDefinitions() {}
}
