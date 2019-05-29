/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransformCheckpointStats {
    public static final ParseField TIMESTAMP_MILLIS = new ParseField("timestamp_millis");
    public static final ParseField TIME_UPPER_BOUND_MILLIS = new ParseField("time_upper_bound_millis");

    public static DataFrameTransformCheckpointStats EMPTY = new DataFrameTransformCheckpointStats(0L, 0L);

    private final long timestampMillis;
    private final long timeUpperBoundMillis;

    public static final ConstructingObjectParser<DataFrameTransformCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_checkpoint_stats", true, args -> {
                long timestamp = args[0] == null ? 0L : (Long) args[0];
                long timeUpperBound = args[1] == null ? 0L : (Long) args[1];

                return new DataFrameTransformCheckpointStats(timestamp, timeUpperBound);
            });

    static {
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_UPPER_BOUND_MILLIS);
    }

    public static DataFrameTransformCheckpointStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    public DataFrameTransformCheckpointStats(final long timestampMillis, final long timeUpperBoundMillis) {
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
    }

    public DataFrameTransformCheckpointStats(StreamInput in) throws IOException {
        this.timestampMillis = in.readLong();
        this.timeUpperBoundMillis = in.readLong();
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public long getTimeUpperBoundMillis() {
        return timeUpperBoundMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, timeUpperBoundMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformCheckpointStats that = (DataFrameTransformCheckpointStats) other;

        return this.timestampMillis == that.timestampMillis && this.timeUpperBoundMillis == that.timeUpperBoundMillis;
    }
}
