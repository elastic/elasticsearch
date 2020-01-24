/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.FunctionalBinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Arithmetics;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Binary math operations. Sister class to {@link MathOperation}.
 */
public class BinaryMathProcessor extends FunctionalBinaryProcessor<Number, Number, Number, BinaryMathOperation> {

    public enum BinaryMathOperation implements BiFunction<Number, Number, Number> {

        ATAN2((l, r) -> Math.atan2(l.doubleValue(), r.doubleValue())),
        MOD(Arithmetics::mod),
        POWER((l, r) -> Math.pow(l.doubleValue(), r.doubleValue()));

        private final BiFunction<Number, Number, Number> process;

        BinaryMathOperation(BiFunction<Number, Number, Number> process) {
            this.process = process;
        }

        @Override
        public final Number apply(Number left, Number right) {
            if (left == null || right == null) {
                return null;
            }
            return process.apply(left, right);
        }
    }
    
    public static final String NAME = "mb";

    public BinaryMathProcessor(Processor left, Processor right, BinaryMathOperation operation) {
        super(left, right, operation);
    }

    public BinaryMathProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(BinaryMathOperation.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void checkParameter(Object param) {
        if (!(param instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", param);
        }
    }
}