/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.literal.interval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.io.IOException;
import java.time.Period;

/**
 * Year/Month (relative) interval.
 */
public class IntervalYearMonth extends Interval<Period> {

    public static final String NAME = "iym";

    private static Period period(StreamInput in) throws IOException {
        return Period.of(in.readVInt(), in.readVInt(), in.readVInt());
    }

    public IntervalYearMonth(Period interval, DataType intervalType) {
        super(interval, intervalType);
    }

    public IntervalYearMonth(StreamInput in) throws IOException {
        super(period(in), SqlDataTypes.fromTypeName(in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Period p = interval();
        out.writeVInt(p.getYears());
        out.writeVInt(p.getMonths());
        out.writeVInt(p.getDays());
        out.writeString(dataType().typeName());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public IntervalYearMonth add(Interval<Period> interval) {
        return new IntervalYearMonth(interval().plus(interval.interval()).normalized(),
                Intervals.compatibleInterval(dataType(), interval.dataType()));
    }

    @Override
    public IntervalYearMonth sub(Interval<Period> interval) {
        return new IntervalYearMonth(interval().minus(interval.interval()).normalized(),
                Intervals.compatibleInterval(dataType(), interval.dataType()));
    }

    @Override
    public Interval<Period> mul(long mul) {
        int i = DataTypeConverter.safeToInt(mul);
        return new IntervalYearMonth(interval().multipliedBy(i), dataType());
    }

    @Override
    public String script() {
        return "{sql}.intervalYearMonth({},{})";
    }
}
