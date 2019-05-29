/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;
import org.elasticsearch.xpack.sql.type.DataTypes;

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

    IntervalYearMonth(StreamInput in) throws IOException {
        super(period(in), in.readEnum(DataType.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Period p = interval();
        out.writeVInt(p.getYears());
        out.writeVInt(p.getMonths());
        out.writeVInt(p.getDays());
        out.writeEnum(dataType());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public IntervalYearMonth add(Interval<Period> interval) {
        return new IntervalYearMonth(interval().plus(interval.interval()).normalized(),
                DataTypes.compatibleInterval(dataType(), interval.dataType()));
    }

    @Override
    public IntervalYearMonth sub(Interval<Period> interval) {
        return new IntervalYearMonth(interval().minus(interval.interval()).normalized(),
                DataTypes.compatibleInterval(dataType(), interval.dataType()));
    }

    @Override
    public Interval<Period> mul(long mul) {
        int i = DataTypeConversion.safeToInt(mul);
        return new IntervalYearMonth(interval().multipliedBy(i), dataType());
    }
}
