/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.processor.FunctionalBinaryProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.literal.Interval;
import org.elasticsearch.xpack.sql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.sql.expression.predicate.PredicateBiFunction;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;

public class BinaryArithmeticProcessor extends FunctionalBinaryProcessor<Object, Object, Object, BinaryArithmeticOperation> {
    
    private interface NumericArithmetic extends BiFunction<Number, Number, Number> {
        default Object wrap(Object l, Object r) {
            return apply((Number) l, (Number) r);
        }
    }

    public enum BinaryArithmeticOperation implements PredicateBiFunction<Object, Object, Object> {
        ADD((Object l, Object r) -> {
            if (l instanceof Number) {
                return Arithmetics.add((Number) l, (Number) r);
            }
            if (l instanceof IntervalYearMonth && r instanceof IntervalYearMonth) {
                return ((IntervalYearMonth) l).add((IntervalYearMonth) r);
            }
            if (l instanceof IntervalDayTime && r instanceof IntervalDayTime) {
                return ((IntervalDayTime) l).add((IntervalDayTime) r);
            }
            l = unwrapJodaTime(l);
            r = unwrapJodaTime(r);
            if (l instanceof ZonedDateTime && r instanceof IntervalYearMonth) {
                return Arithmetics.add((ZonedDateTime) l, ((IntervalYearMonth) r).interval());
            }
            if (l instanceof ZonedDateTime && r instanceof IntervalDayTime) {
                return Arithmetics.add((ZonedDateTime) l, ((IntervalDayTime) r).interval());
            }
            if (r instanceof ZonedDateTime && l instanceof IntervalYearMonth) {
                return Arithmetics.add((ZonedDateTime) r, ((IntervalYearMonth) l).interval());
            }
            if (r instanceof ZonedDateTime && l instanceof IntervalDayTime) {
                return Arithmetics.add((ZonedDateTime) r, ((IntervalDayTime) l).interval());
            }

            throw new SqlIllegalArgumentException("Cannot compute [+] between [{}] [{}]", l.getClass().getSimpleName(),
                    r.getClass().getSimpleName());
        }, "+"),
        SUB((Object l, Object r) -> {
            if (l instanceof Number) {
                return Arithmetics.sub((Number) l, (Number) r);
            }
            if (l instanceof IntervalYearMonth && r instanceof IntervalYearMonth) {
                return ((IntervalYearMonth) l).sub((IntervalYearMonth) r);
            }
            if (l instanceof IntervalDayTime && r instanceof IntervalDayTime) {
                return ((IntervalDayTime) l).sub((IntervalDayTime) r);
            }
            l = unwrapJodaTime(l);
            r = unwrapJodaTime(r);
            if (l instanceof ZonedDateTime && r instanceof IntervalYearMonth) {
                return Arithmetics.sub((ZonedDateTime) l, ((IntervalYearMonth) r).interval());
            }
            if (l instanceof ZonedDateTime && r instanceof IntervalDayTime) {
                return Arithmetics.sub((ZonedDateTime) l, ((IntervalDayTime) r).interval());
            }
            if (r instanceof ZonedDateTime && l instanceof Interval<?>) {
                throw new SqlIllegalArgumentException("Cannot substract a date from an interval; do you mean the reverse?");
            }

            throw new SqlIllegalArgumentException("Cannot compute [-] between [{}] [{}]", l.getClass().getSimpleName(),
                    r.getClass().getSimpleName());
        }, "-"),
        MUL(Arithmetics::mul, "*"),
        DIV(Arithmetics::div, "/"),
        MOD(Arithmetics::mod, "%");

        private final BiFunction<Object, Object, Object> process;
        private final String symbol;

        BinaryArithmeticOperation(BiFunction<Object, Object, Object> process, String symbol) {
            this.process = process;
            this.symbol = symbol;
        }

        BinaryArithmeticOperation(NumericArithmetic process, String symbol) {
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

        private static Object unwrapJodaTime(Object o) {
            return o instanceof JodaCompatibleZonedDateTime ? ((JodaCompatibleZonedDateTime) o).getZonedDateTime() : o;
        }
    }
    
    public static final String NAME = "abn";

    public BinaryArithmeticProcessor(Processor left, Processor right, BinaryArithmeticOperation operation) {
        super(left, right, operation);
    }

    public BinaryArithmeticProcessor(StreamInput in) throws IOException {
        super(in, i -> i.readEnum(BinaryArithmeticOperation.class));
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

        if (f == BinaryArithmeticOperation.MUL || f == BinaryArithmeticOperation.DIV || f == BinaryArithmeticOperation.MOD) {
            if (!(left instanceof Number)) {
                throw new SqlIllegalArgumentException("A number is required; received {}", left);
            }

            if (!(right instanceof Number)) {
                throw new SqlIllegalArgumentException("A number is required; received {}", right);
            }

            return f.apply(left, right);
        }

        if (f == BinaryArithmeticOperation.ADD || f == BinaryArithmeticOperation.SUB) {
                return f.apply(left, right);
        }

        // this should not occur
        throw new SqlIllegalArgumentException("Cannot perform arithmetic operation due to arguments");
    }
}