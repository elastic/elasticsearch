/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Class representing an (inclusive) range of {@code long} values in a field in a single shard.
 */
public class ShardLongFieldRange implements Writeable {

    public static final Version LONG_FIELD_RANGE_VERSION_INTRODUCED = Version.V_8_0_0;

    /**
     * Sentinel value indicating an empty range, for instance because the field is missing or has no values.
     */
    public static final ShardLongFieldRange EMPTY = new ShardLongFieldRange(Long.MAX_VALUE, Long.MIN_VALUE);

    /**
     * Sentinel value indicating the actual range may change in the future.
     */
    public static final ShardLongFieldRange MUTABLE = new ShardLongFieldRange(Long.MIN_VALUE, Long.MAX_VALUE);

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
        assert this != EMPTY && this != MUTABLE && min <= max: "must not use actual min of sentinel values";
        return min;
    }

    /**
     * @return the (inclusive) maximum of this range.
     */
    public long getMax() {
        assert this != EMPTY && this != MUTABLE && min <= max : "must not use actual max of sentinel values";
        return max;
    }

    @Override
    public String toString() {
        if (this == MUTABLE) {
            return "MUTABLE";
        } else if (this == EMPTY) {
            return "EMPTY";
        } else {
            return "[" + min + "-" + max + "]";
        }
    }

    public static ShardLongFieldRange readFrom(StreamInput in) throws IOException {
        if (in.getVersion().before(LONG_FIELD_RANGE_VERSION_INTRODUCED)) {
            // conservative treatment for BWC
            return MUTABLE;
        }

        final byte type = in.readByte();
        switch (type) {
            case 1:
                return MUTABLE;
            case 2:
                return EMPTY;
            case 0:
                return ShardLongFieldRange.of(in.readZLong(), in.readZLong());
            default:
                throw new IllegalStateException("type [" + type + "] not known");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LONG_FIELD_RANGE_VERSION_INTRODUCED)) {
            if (this == MUTABLE) {
                out.writeByte((byte) 1);
            } else if (this == EMPTY) {
                out.writeByte((byte) 2);
            } else {
                out.writeByte((byte) 0);
                out.writeZLong(min);
                out.writeZLong(max);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (this == EMPTY || this == MUTABLE || o == EMPTY || o == MUTABLE) return false;
        final ShardLongFieldRange that = (ShardLongFieldRange) o;
        return min == that.min && max == that.max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }
}

