/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.type.DataType;

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
public abstract class Interval<I extends TemporalAmount> implements ToXContentObject {

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
        builder.value(interval);
        return builder;
    }

    @Override
    public String toString() {
        return intervalType.name() + "[" + interval + "]";
    }
}