/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Class encapsulating stats about the circuit breaker
 */
public class CircuitBreakerStats implements Writeable, ToXContentObject {

    private final String name;
    private final long limit;
    private final long estimated;
    private final long trippedCount;
    private final double overhead;

    public CircuitBreakerStats(String name, long limit, long estimated, double overhead, long trippedCount) {
        this.name = name;
        this.limit = limit;
        this.estimated = estimated;
        this.trippedCount = trippedCount;
        this.overhead = overhead;
    }

    public CircuitBreakerStats(StreamInput in) throws IOException {
        this.limit = in.readLong();
        this.estimated = in.readLong();
        this.overhead = in.readDouble();
        this.trippedCount = in.readLong();
        this.name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(limit);
        out.writeLong(estimated);
        out.writeDouble(overhead);
        out.writeLong(trippedCount);
        out.writeString(name);
    }

    public String getName() {
        return this.name;
    }

    public long getLimit() {
        return this.limit;
    }

    public long getEstimated() {
        return this.estimated;
    }

    public long getTrippedCount() {
        return this.trippedCount;
    }

    public double getOverhead() {
        return this.overhead;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name.toLowerCase(Locale.ROOT));
        addBytesFieldsSafe(builder, limit, Fields.LIMIT, Fields.LIMIT_HUMAN);
        addBytesFieldsSafe(builder, estimated, Fields.ESTIMATED, Fields.ESTIMATED_HUMAN);
        builder.field(Fields.OVERHEAD, overhead);
        builder.field(Fields.TRIPPED_COUNT, trippedCount);
        builder.endObject();
        return builder;
    }

    private void addBytesFieldsSafe(XContentBuilder builder, long bytes, String rawFieldName, String humanFieldName) throws IOException {
        builder.field(rawFieldName, bytes);
        if (-1L <= bytes) {
            builder.field(humanFieldName, ByteSizeValue.ofBytes(bytes));
        } else {
            // Something's definitely wrong, maybe a breaker was freed twice? Still, we're just writing out stats here, so we should keep
            // going if we're running in production.
            assert HierarchyCircuitBreakerService.permitNegativeValues : this;
            // noinspection ResultOfMethodCallIgnored - we call toString() to log a warning
            toString();
            builder.field(humanFieldName, "");
        }
    }

    @Override
    public String toString() {
        final var stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        stringBuilder.append(this.name);
        stringBuilder.append(",limit=");
        HierarchyCircuitBreakerService.appendBytesSafe(stringBuilder, this.limit);
        stringBuilder.append(",estimated=");
        HierarchyCircuitBreakerService.appendBytesSafe(stringBuilder, this.estimated);
        stringBuilder.append(",overhead=");
        stringBuilder.append(this.overhead);
        stringBuilder.append(",tripped=");
        stringBuilder.append(this.trippedCount);
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    static final class Fields {
        static final String LIMIT = "limit_size_in_bytes";
        static final String LIMIT_HUMAN = "limit_size";
        static final String ESTIMATED = "estimated_size_in_bytes";
        static final String ESTIMATED_HUMAN = "estimated_size";
        static final String OVERHEAD = "overhead";
        static final String TRIPPED_COUNT = "tripped";
    }
}
