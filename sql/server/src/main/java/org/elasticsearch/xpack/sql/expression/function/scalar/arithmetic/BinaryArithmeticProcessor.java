/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;

import static java.lang.String.format;

public class BinaryArithmeticProcessor extends BinaryProcessor {
    
    public enum BinaryArithmeticOperation {

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

        public final Number apply(Number left, Number right) {
            return process.apply(left, right);
        }

        @Override
        public String toString() {
            return symbol;
        }
    }
    
    public static final String NAME = "ab";

    private final BinaryArithmeticOperation operation;

    public BinaryArithmeticProcessor(Processor left, Processor right, BinaryArithmeticOperation operation) {
        super(left, right);
        this.operation = operation;
    }

    public BinaryArithmeticProcessor(StreamInput in) throws IOException {
        super(in);
        operation = in.readEnum(BinaryArithmeticOperation.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(operation);
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        return operation.apply((Number) left, (Number) right);
    }

    @Override
    public int hashCode() {
        return operation.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        BinaryArithmeticProcessor other = (BinaryArithmeticProcessor) obj;
        return Objects.equals(operation, other.operation)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "(%s %s %s)", left(), operation, right());
    }
}