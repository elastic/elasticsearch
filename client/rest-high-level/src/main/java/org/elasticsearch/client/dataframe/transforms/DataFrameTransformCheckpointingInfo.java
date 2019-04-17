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

import java.util.Objects;

public class DataFrameTransformCheckpointingInfo {

    public static final ParseField CURRENT_CHECKPOINT = new ParseField("current");
    public static final ParseField IN_PROGRESS_CHECKPOINT = new ParseField("in_progress");
    public static final ParseField OPERATIONS_BEHIND = new ParseField("operations_behind");

    private final DataFrameTransformCheckpointStats current;
    private final DataFrameTransformCheckpointStats inProgress;
    private final long operationsBehind;


    private static final ConstructingObjectParser<DataFrameTransformCheckpointingInfo, Void> LENIENT_PARSER =
            new ConstructingObjectParser<>(
                    "data_frame_transform_checkpointing_info", true, a -> {
                        long behind = a[2] == null ? 0L : (Long) a[2];

                        return new DataFrameTransformCheckpointingInfo(
                                a[0] == null ? DataFrameTransformCheckpointStats.EMPTY : (DataFrameTransformCheckpointStats) a[0],
                                a[1] == null ? DataFrameTransformCheckpointStats.EMPTY : (DataFrameTransformCheckpointStats) a[1], behind);
                    });

    static {
        LENIENT_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> DataFrameTransformCheckpointStats.fromXContent(p), CURRENT_CHECKPOINT);
        LENIENT_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> DataFrameTransformCheckpointStats.fromXContent(p), IN_PROGRESS_CHECKPOINT);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), OPERATIONS_BEHIND);
    }

    public DataFrameTransformCheckpointingInfo(DataFrameTransformCheckpointStats current, DataFrameTransformCheckpointStats inProgress,
            long operationsBehind) {
        this.current = Objects.requireNonNull(current);
        this.inProgress = Objects.requireNonNull(inProgress);
        this.operationsBehind = operationsBehind;
    }

    public DataFrameTransformCheckpointStats getCurrent() {
        return current;
    }

    public DataFrameTransformCheckpointStats getInProgress() {
        return inProgress;
    }

    public long getOperationsBehind() {
        return operationsBehind;
    }

    public static DataFrameTransformCheckpointingInfo fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, inProgress, operationsBehind);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformCheckpointingInfo that = (DataFrameTransformCheckpointingInfo) other;

        return Objects.equals(this.current, that.current) &&
                Objects.equals(this.inProgress, that.inProgress) &&
                this.operationsBehind == that.operationsBehind;
    }

}
