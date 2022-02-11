/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Arithmetics;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Arithmetics.NumericArithmetic;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.expression.literal.interval.Interval;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalArithmetics;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;

import java.io.IOException;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToLong;

public enum SqlBinaryArithmeticOperation implements BinaryArithmeticOperation {

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
        if ((l instanceof ZonedDateTime || l instanceof OffsetTime) && r instanceof IntervalYearMonth) {
            return IntervalArithmetics.add((Temporal) l, ((IntervalYearMonth) r).interval());
        }
        if ((l instanceof ZonedDateTime || l instanceof OffsetTime) && r instanceof IntervalDayTime) {
            return IntervalArithmetics.add((Temporal) l, ((IntervalDayTime) r).interval());
        }
        if ((r instanceof ZonedDateTime || r instanceof OffsetTime) && l instanceof IntervalYearMonth) {
            return IntervalArithmetics.add((Temporal) r, ((IntervalYearMonth) l).interval());
        }
        if ((r instanceof ZonedDateTime || r instanceof OffsetTime) && l instanceof IntervalDayTime) {
            return IntervalArithmetics.add((Temporal) r, ((IntervalDayTime) l).interval());
        }

        throw new QlIllegalArgumentException(
            "Cannot compute [+] between [{}] and [{}]",
            l.getClass().getSimpleName(),
            r.getClass().getSimpleName()
        );
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
        if ((l instanceof ZonedDateTime || l instanceof OffsetTime) && r instanceof IntervalYearMonth) {
            return IntervalArithmetics.sub((Temporal) l, ((IntervalYearMonth) r).interval());
        }
        if ((l instanceof ZonedDateTime || l instanceof OffsetTime) && r instanceof IntervalDayTime) {
            return IntervalArithmetics.sub((Temporal) l, ((IntervalDayTime) r).interval());
        }
        if ((r instanceof ZonedDateTime || r instanceof OffsetTime) && l instanceof Interval<?>) {
            throw new QlIllegalArgumentException("Cannot subtract a date from an interval; do you mean the reverse?");
        }

        throw new QlIllegalArgumentException(
            "Cannot compute [-] between [{}] and [{}]",
            l.getClass().getSimpleName(),
            r.getClass().getSimpleName()
        );
    }, "-"),
    MUL((Object l, Object r) -> {
        if (l instanceof Number && r instanceof Number) {
            return Arithmetics.mul((Number) l, (Number) r);
        }
        if (l instanceof Number number && r instanceof IntervalYearMonth) {
            return ((IntervalYearMonth) r).mul(safeToLong(number));
        }
        if (r instanceof Number number && l instanceof IntervalYearMonth) {
            return ((IntervalYearMonth) l).mul(safeToLong(number));
        }
        if (l instanceof Number number && r instanceof IntervalDayTime) {
            return ((IntervalDayTime) r).mul(safeToLong(number));
        }
        if (r instanceof Number number && l instanceof IntervalDayTime) {
            return ((IntervalDayTime) l).mul(safeToLong(number));
        }

        throw new QlIllegalArgumentException(
            "Cannot compute [*] between [{}] and [{}]",
            l.getClass().getSimpleName(),
            r.getClass().getSimpleName()
        );
    }, "*"),
    DIV(Arithmetics::div, "/"),
    MOD(Arithmetics::mod, "%");

    public static final String NAME = "abn-sql";

    private final BiFunction<Object, Object, Object> process;
    private final String symbol;

    SqlBinaryArithmeticOperation(BiFunction<Object, Object, Object> process, String symbol) {
        this.process = process;
        this.symbol = symbol;
    }

    SqlBinaryArithmeticOperation(NumericArithmetic process, String symbol) {
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

    public static SqlBinaryArithmeticOperation read(StreamInput in) throws IOException {
        return in.readEnum(SqlBinaryArithmeticOperation.class);
    }
}
