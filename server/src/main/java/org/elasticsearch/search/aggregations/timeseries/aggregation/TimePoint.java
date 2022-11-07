/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class TimePoint implements Comparable<TimePoint>, Writeable {
    private long timestamp;
    private double value;

    public TimePoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public TimePoint(StreamInput in) throws IOException {
        this.timestamp = in.readLong();
        this.value = in.readDouble();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Point{" + "timestamp=" + timestamp + ", value=" + value + '}';
    }

    @Override
    public int compareTo(TimePoint o) {
        return (int) (this.timestamp - o.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimePoint timePoint = (TimePoint) o;
        return timestamp == timePoint.timestamp && Objects.equals(value, timePoint.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(value);
    }
}
