/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal.interval;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ql.expression.function.scalar.IntervalScripting;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantNamedWriteable;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.io.IOException;
import java.time.temporal.TemporalAmount;
import java.util.Objects;

/**
 * Interval value.
 * 
 * As SQL defines two main types, YearMonth and DayToHour/Minute/Second, the interval has to be split accordingly
 * mainly to differentiate between a period (which is relative) for the former and duration (which is exact)
 * for the latter.
 * Unfortunately because the SQL interval type is not preserved accurately by the JDK TemporalAmount class
 * in both cases, the data type needs to be carried around as it cannot be inferred.
 */
public abstract class Interval<I extends TemporalAmount> implements ConstantNamedWriteable, ToXContentObject, IntervalScripting {

    private final I interval;
    private final DataType intervalType;

    public Interval(I interval, DataType intervalType) {
        this.interval = interval;
        this.intervalType = intervalType;
    }

    public I interval() {
        return interval;
    }

    public DataType dataType() {
        return intervalType;
    }

    public abstract Interval<I> add(Interval<I> interval);

    public abstract Interval<I> sub(Interval<I> interval);

    public abstract Interval<I> mul(long mul);

    @Override
    public int hashCode() {
        return Objects.hash(interval, intervalType);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Interval<?> other = (Interval<?>) obj;
        return Objects.equals(other.interval, interval)
            && Objects.equals(other.intervalType, intervalType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(interval);
    }

    @Override
    public String toString() {
        return DateUtils.toString(interval);
    }

    @Override
    public String value() {
        return interval().toString();
    }

    @Override
    public String typeName() {
        return dataType().toString();
    }
}