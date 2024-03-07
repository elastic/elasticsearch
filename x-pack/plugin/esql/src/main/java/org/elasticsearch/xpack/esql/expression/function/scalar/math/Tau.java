/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.Source;

import java.util.List;

/**
 * Function that emits tau, also known as 2 * pi.
 */
public class Tau extends DoubleConstantFunction {
    public static final double TAU = Math.PI * 2;

    @FunctionInfo(returnType = "double", description = "The ratio of a circle’s circumference to its radius.")
    public Tau(Source source) {
        super(source);
    }

    @Override
    public Object fold() {
        return TAU;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Tau(source());
    }
}
