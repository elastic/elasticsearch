/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryNumericProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;

import java.io.IOException;
import java.util.function.BiFunction;

public class BinaryArithmeticProcessor extends BinaryNumericProcessor<BinaryArithmeticOperation> {
    
    public enum BinaryArithmeticOperation implements BiFunction<Number, Number, Number> {

        ADD(Arithmetics::add, "+"),
        SUB(Arithmetics::sub, "-"),
        MUL(Arithmetics::mul, "*"),
        DIV(Arithmetics::div, "/"),
        MOD(Arithmetics::mod, "%");

        private final BiFunction<Number, Number, Number> process;
        private final String symbol;

        BinaryArithmeticOperation(BiFunction<Number, Number, Number> process, String symbol) {
            this.process = process;
            this.symbol = symbol;
        }

        public String symbol() {
            return symbol;
        }

        @Override
        public final Number apply(Number left, Number right) {
            return process.apply(left, right);
        }

        @Override
        public String toString() {
            return symbol;
        }
    }
    
    public static final String NAME = "ab";

    public BinaryArithmeticProcessor(Processor left, Processor right, BinaryArithmeticOperation operation) {
        super(left, right, operation);
    }

    public BinaryArithmeticProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(BinaryArithmeticOperation.class));
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(operation());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}