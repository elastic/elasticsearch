/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Processor for binary mathematical operations that have a second optional parameter.
 */
public class BinaryOptionalMathProcessor implements Processor {
    
    public enum BinaryOptionalMathOperation implements BiFunction<Number, Number, Number> {

        ROUND((l, r) -> {
            double tenAtScale = Math.pow(10., r.longValue());
            double middleResult = l.doubleValue() * tenAtScale;
            int sign = middleResult > 0 ? 1 : -1;
            return Math.round(Math.abs(middleResult)) / tenAtScale * sign;
        }),
        TRUNCATE((l, r) -> {
            double tenAtScale = Math.pow(10., r.longValue());
            double g = l.doubleValue() * tenAtScale;
            return (((l.doubleValue() < 0) ? Math.ceil(g) : Math.floor(g)) / tenAtScale);
        });

        private final BiFunction<Number, Number, Number> process;

        BinaryOptionalMathOperation(BiFunction<Number, Number, Number> process) {
            this.process = process;
        }

        @Override
        public final Number apply(Number left, Number right) {
            if (left == null) {
                return null;
            }
            if (!(left instanceof Number)) {
                throw new SqlIllegalArgumentException("A number is required; received [{}]", left);
            }

            if (right != null) {
                if (!(right instanceof Number)) {
                    throw new SqlIllegalArgumentException("A number is required; received [{}]", right);
                }
                if (right instanceof Float || right instanceof Double) {
                    throw new SqlIllegalArgumentException("An integer number is required; received [{}] as second parameter", right);
                }
            } else {
                right = 0;
            }
            
            return process.apply(left, right);
        }
    }

    private final Processor left, right;
    private final BinaryOptionalMathOperation operation;
    public static final String NAME = "mob";

    public BinaryOptionalMathProcessor(Processor left, Processor right, BinaryOptionalMathOperation operation) {
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public BinaryOptionalMathProcessor(StreamInput in) throws IOException {
        left = in.readNamedWriteable(Processor.class);
        right = in.readOptionalNamedWriteable(Processor.class);
        operation = in.readEnum(BinaryOptionalMathOperation.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(left);
        out.writeOptionalNamedWriteable(right);
        out.writeEnum(operation);
    }

    @Override
    public Object process(Object input) {
        return doProcess(left().process(input), right() == null ? null : right().process(input));
    }

    public Number doProcess(Object left, Object right) {
        if (left == null) {
            return null;
        }
        if (!(left instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", left);
        }

        if (right != null) {
            if (!(right instanceof Number)) {
                throw new SqlIllegalArgumentException("A number is required; received [{}]", right);
            }
            if (right instanceof Float || right instanceof Double) {
                throw new SqlIllegalArgumentException("An integer number is required; received [{}] as second parameter", right);
            }
        } else {
            right = 0;
        }

        return operation().apply((Number) left, (Number) right);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        BinaryOptionalMathProcessor other = (BinaryOptionalMathProcessor) obj;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right())
                && Objects.equals(operation(), other.operation());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), operation());
    }
    
    public Processor left() {
        return left;
    }
    
    public Processor right() {
        return right;
    }
    
    public BinaryOptionalMathOperation operation() {
        return operation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
