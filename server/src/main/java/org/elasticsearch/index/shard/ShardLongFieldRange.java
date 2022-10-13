/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Class representing an (inclusive) range of {@code long} values in a field in a single shard.
 */
public class ShardLongFieldRange implements Writeable {

    /**
     * Sentinel value indicating an empty range, for instance because the field is missing or has no values.
     */
    public static final ShardLongFieldRange EMPTY = new ShardLongFieldRange(Long.MAX_VALUE, Long.MIN_VALUE);

    /**
     * Sentinel value indicating the actual range is unknown, for instance because more docs may be added in future.
     */
    public static final ShardLongFieldRange UNKNOWN = new ShardLongFieldRange(Long.MIN_VALUE, Long.MAX_VALUE);

    /**
     * Construct a new {@link ShardLongFieldRange} with the given (inclusive) minimum and maximum.
     */
    public static ShardLongFieldRange of(long min, long max) {
        assert min <= max : min + " vs " + max;
        return new ShardLongFieldRange(min, max);
    }

    private final long min, max;

    private ShardLongFieldRange(long min, long max) {
        this.min = min;
        this.max = max;
    }

    /**
     * @return the (inclusive) minimum of this range.
     */
    public long getMin() {
        assert this != EMPTY && this != UNKNOWN && min <= max : "must not use actual min of sentinel values";
        return min;
    }

    /**
     * @return the (inclusive) maximum of this range.
     */
    public long getMax() {
        assert this != EMPTY && this != UNKNOWN && min <= max : "must not use actual max of sentinel values";
        return max;
    }

    @Override
    public String toString() {
        if (this == UNKNOWN) {
            return "UNKNOWN";
        } else if (this == EMPTY) {
            return "EMPTY";
        } else {
            return "[" + min + "-" + max + "]";
        }
    }

    private static final byte WIRE_TYPE_OTHER = (byte) 0;
    private static final byte WIRE_TYPE_UNKNOWN = (byte) 1;
    private static final byte WIRE_TYPE_EMPTY = (byte) 2;

    public static ShardLongFieldRange readFrom(StreamInput in) throws IOException {
        final byte type = in.readByte();
        return switch (type) {
            case WIRE_TYPE_UNKNOWN -> UNKNOWN;
            case WIRE_TYPE_EMPTY -> EMPTY;
            case WIRE_TYPE_OTHER -> ShardLongFieldRange.of(in.readZLong(), in.readZLong());
            default -> throw new IllegalStateException("type [" + type + "] not known");
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this == UNKNOWN) {
            out.writeByte(WIRE_TYPE_UNKNOWN);
        } else if (this == EMPTY) {
            out.writeByte(WIRE_TYPE_EMPTY);
        } else {
            out.writeByte(WIRE_TYPE_OTHER);
            out.writeZLong(min);
            out.writeZLong(max);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (this == EMPTY || this == UNKNOWN || o == EMPTY || o == UNKNOWN) return false;
        final ShardLongFieldRange that = (ShardLongFieldRange) o;
        return min == that.min && max == that.max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }
}
