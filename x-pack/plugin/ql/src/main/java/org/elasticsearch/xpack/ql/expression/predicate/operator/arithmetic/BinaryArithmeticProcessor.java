/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.function.BiFunction;

public class BinaryArithmeticProcessor extends FunctionalBinaryProcessor<Object, Object, Object, BinaryArithmeticOperation> {
    
    private interface NumericArithmetic extends BiFunction<Number, Number, Number> {
        default Object wrap(Object l, Object r) {
            return apply((Number) l, (Number) r);
        }
    }

    public enum DefaultBinaryArithmeticOperation implements BinaryArithmeticOperation {
        ADD(Arithmetics::add, "+"),
        SUB(Arithmetics::sub, "-"),
        MUL(Arithmetics::mul, "*"),
        DIV(Arithmetics::div, "/"),
        MOD(Arithmetics::mod, "%");

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
        public void doWrite(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }
    
    public static final String NAME = "abn";

    public BinaryArithmeticProcessor(Processor left, Processor right, BinaryArithmeticOperation operation) {
        super(left, right, operation);
    }

    public BinaryArithmeticProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(DefaultBinaryArithmeticOperation.class));
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        function().doWrite(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        BinaryArithmeticOperation f = function();

        if (left == null || right == null) {
            return null;
        }

        if (!(left instanceof Number)) {
            throw new QlIllegalArgumentException("A number is required; received {}", left);
        }

        if (!(right instanceof Number)) {
            throw new QlIllegalArgumentException("A number is required; received {}", right);
        }

        return f.apply(left, right);
    }
}