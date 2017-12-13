/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import java.io.IOException;
import java.util.function.DoubleFunction;
import java.util.function.Function;

public class MathProcessor implements Processor {
    
    public enum MathOperation {
        ABS((Object l) -> {
            if (l instanceof Float) {
                return Math.abs(((Float) l).floatValue());
            }
            if (l instanceof Double) {
                return Math.abs(((Double) l).doubleValue());
            }
            long lo = ((Number) l).longValue();
            return lo >= 0 ? lo : lo == Long.MIN_VALUE ? Long.MAX_VALUE : -lo;
        }),

        ACOS(Math::acos),
        ASIN(Math::asin),
        ATAN(Math::atan),
        CBRT(Math::cbrt),
        CEIL(Math::ceil),
        COS(Math::cos),
        COSH(Math::cosh),
        DEGREES(Math::toDegrees),
        E((Object l) -> Math.E),
        EXP(Math::exp),
        EXPM1(Math::expm1),
        FLOOR(Math::floor),
        LOG(Math::log),
        LOG10(Math::log10),
        PI((Object l) -> Math.PI),
        RADIANS(Math::toRadians),
        ROUND((DoubleFunction<Object>) Math::round),
        SIN(Math::sin),
        SINH(Math::sinh),
        SQRT(Math::sqrt),
        TAN(Math::tan);

        private final Function<Object, Object> apply;

        MathOperation(Function<Object, Object> apply) {
            this.apply = apply;
        }

        MathOperation(DoubleFunction<Object> apply) {
            this.apply = (Object l) -> apply.apply(((Number) l).doubleValue());
        }

        public final Object apply(Object l) {
            return apply.apply(l);
        }
    }
    
    public static final String NAME = "m";

    private final MathOperation processor;

    public MathProcessor(MathOperation processor) {
        this.processor = processor;
    }

    public MathProcessor(StreamInput in) throws IOException {
        processor = in.readEnum(MathOperation.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return processor.apply(input);
    }

    MathOperation processor() {
        return processor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MathProcessor other = (MathProcessor) obj;
        return processor == other.processor;
    }

    @Override
    public int hashCode() {
        return processor.hashCode();
    }

    @Override
    public String toString() {
        return processor.toString();
    }
}