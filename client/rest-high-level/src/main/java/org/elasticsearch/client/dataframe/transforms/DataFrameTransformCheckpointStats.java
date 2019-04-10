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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransformCheckpointStats {
    public static final ParseField TIMESTAMP_MILLIS = new ParseField("timestamp_millis");
    public static final ParseField TIME_UPPER_BOUND_MILLIS = new ParseField("time_upper_bound_millis");
    public static final ParseField TOTAL_DOCS = new ParseField("total_docs");
    public static final ParseField COMPLETED_DOCS = new ParseField("completed_docs");
    public static DataFrameTransformCheckpointStats EMPTY = new DataFrameTransformCheckpointStats(0L, 0L, 0L, 0L);

    private final long timestampMillis;
    private final long timeUpperBoundMillis;
    private final long totalDocs;
    private final long completedDocs;

    public static final ConstructingObjectParser<DataFrameTransformCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_checkpoint_stats", true, args -> {
                long timestamp = args[0] == null ? 0L : (Long) args[0];
                long timeUpperBound = args[1] == null ? 0L : (Long) args[1];
                long totalDocs = args[2] == null ? 0L : (Long) args[2];
                long completedDocs = args[3] == null ? 0L : (Long) args[3];

                return new DataFrameTransformCheckpointStats(timestamp, timeUpperBound, totalDocs, completedDocs);
            });

    static {
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_UPPER_BOUND_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TOTAL_DOCS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), COMPLETED_DOCS);
    }

    public static DataFrameTransformCheckpointStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    public DataFrameTransformCheckpointStats(final long timestampMillis,
                                             final long timeUpperBoundMillis,
                                             final long totalDocs,
                                             final long completedDocs) {
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
        this.totalDocs = totalDocs;
        this.completedDocs = completedDocs;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public long getTimeUpperBoundMillis() {
        return timeUpperBoundMillis;
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    public long getCompletedDocs() {
        return completedDocs;
    }

    public double getPercentageCompleted() {
        if (completedDocs >= totalDocs) {
            return 1.0;
        }
        return (double)completedDocs/totalDocs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, timeUpperBoundMillis, completedDocs, totalDocs);
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

        return this.timestampMillis == that.timestampMillis
            && this.timeUpperBoundMillis == that.timeUpperBoundMillis
            && this.totalDocs == that.totalDocs
            && this.completedDocs == that.completedDocs;
    }
}
