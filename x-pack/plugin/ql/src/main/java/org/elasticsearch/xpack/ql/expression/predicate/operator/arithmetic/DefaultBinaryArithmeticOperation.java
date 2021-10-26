/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Arithmetics.NumericArithmetic;

import java.io.IOException;
import java.util.function.BiFunction;

public enum DefaultBinaryArithmeticOperation implements BinaryArithmeticOperation {

    ADD(Arithmetics::add, "+"),
    SUB(Arithmetics::sub, "-"),
    MUL(Arithmetics::mul, "*"),
    DIV(Arithmetics::div, "/"),
    MOD(Arithmetics::mod, "%");

    public static final String NAME = "abn-def";

    private final BiFunction<Object, Object, Object> process;
    private final String symbol;

    DefaultBinaryArithmeticOperation(BiFunction<Object, Object, Object> process, String symbol) {
        this.process = process;
        this.symbol = symbol;
    }

    DefaultBinaryArithmeticOperation(NumericArithmetic process, String symbol) {
        this(process::wrap, symbol);
    }

    @Override
    public String symbol() {
        return symbol;
    }

    @Override
    public final Object doApply(Object left, Object right) {
        return process.apply(left, right);
    }

    @Override
    public String toString() {
        return symbol;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static DefaultBinaryArithmeticOperation read(StreamInput in) throws IOException {
        return in.readEnum(DefaultBinaryArithmeticOperation.class);
    }
}
