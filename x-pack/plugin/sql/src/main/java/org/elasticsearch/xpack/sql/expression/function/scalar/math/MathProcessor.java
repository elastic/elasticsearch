/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class MathProcessor implements Processor {

    public enum MathOperation {
        ABS((Object l) -> {
            if (l instanceof Double) {
                return Math.abs(((Double) l).doubleValue());
            }
            if (l instanceof Float) {
                return Math.abs(((Float) l).floatValue());
            }
            if (l instanceof BigInteger) {
                return ((BigInteger) l).abs();
            }

            // fallback to integer
            long lo = ((Number) l).longValue();

            if (lo == Long.MIN_VALUE) {
                throw new InvalidArgumentException("[" + lo + "] cannot be negated since the result is outside the range");
            }

            lo = lo < 0 ? -lo : lo;

            if (l instanceof Integer) {
                if ((int) lo == Integer.MIN_VALUE) {
                    throw new InvalidArgumentException("[" + lo + "] cannot be negated since the result is outside the range");
                }
                return DataTypeConverter.safeToInt(lo);
            }

            if (l instanceof Short) {
                return DataTypeConverter.safeToShort(lo);
            }
            if (l instanceof Byte) {
                return DataTypeConverter.safeToByte(lo);
            }

            return lo;
        }),
        ACOS(Math::acos),
        ASIN(Math::asin),
        ATAN(Math::atan),
        CBRT(Math::cbrt),
        CEIL(Math::ceil),
        COS(Math::cos),
        COSH(Math::cosh),
        COT((Object l) -> 1.0d / Math.tan(((Number) l).doubleValue())),
        DEGREES(Math::toDegrees),
        E(() -> Math.E),
        EXP(Math::exp),
        EXPM1(Math::expm1),
        FLOOR(Math::floor),
        LOG(Math::log),
        LOG10(Math::log10),
        PI(() -> Math.PI),
        RADIANS(Math::toRadians),
        RANDOM((Object l) -> new Random(((Number) l).longValue()).nextDouble()),
        SIGN((Object l) -> {
            if (l instanceof Double) {
                return (int) Math.signum((Double) l);
            }
            if (l instanceof Float) {
                return (int) Math.signum((Float) l);
            }
            if (l instanceof BigInteger) {
                return ((BigInteger) l).signum();
            }

            return Long.signum(((Number) l).longValue());
        }),
        SIN(Math::sin),
        SINH(Math::sinh),
        SQRT(Math::sqrt),
        TAN(Math::tan);

        private final Function<Object, Number> apply;

        MathOperation(Function<Object, Number> apply) {
            this.apply = l -> l == null ? null : apply.apply(l);
        }

        MathOperation(DoubleFunction<Double> apply) {
            this.apply = (Object l) -> l == null ? null : apply.apply(((Number) l).doubleValue());
        }

        MathOperation(Supplier<Double> supplier) {
            this.apply = l -> supplier.get();
        }

        public final Number apply(Object l) {
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
        if (input != null && (input instanceof Number) == false) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", input);
        }

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
