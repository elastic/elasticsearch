/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.io.IOException;
import java.time.Duration;

/**
 * Day/Hour/Minutes/Seconds (exact) interval.
 */
public class IntervalDayTime extends Interval<Duration> {

    public static final String NAME = "idt";

    private static Duration duration(StreamInput in) throws IOException {
        return Duration.ofSeconds(in.readVLong(), in.readVInt());
    }

    public IntervalDayTime(Duration interval, DataType intervalType) {
        super(interval, intervalType);
    }

    IntervalDayTime(StreamInput in) throws IOException {
        super(duration(in), in.readEnum(DataType.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(interval().getSeconds());
        out.writeVInt(interval().getNano());
        out.writeEnum(dataType());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public IntervalDayTime add(Interval<Duration> interval) {
        return new IntervalDayTime(interval().plus(interval.interval()), DataTypes.compatibleInterval(dataType(), interval.dataType()));
    }

    @Override
    public IntervalDayTime sub(Interval<Duration> interval) {
        return new IntervalDayTime(interval().minus(interval.interval()), DataTypes.compatibleInterval(dataType(), interval.dataType()));
    }

    @Override
    public Interval<Duration> mul(long mul) {
        return new IntervalDayTime(interval().multipliedBy(mul), dataType());
    }
}
