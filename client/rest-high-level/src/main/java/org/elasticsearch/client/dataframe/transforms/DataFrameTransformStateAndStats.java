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

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransformStateAndStats {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField CHECKPOINTING_INFO_FIELD = new ParseField("checkpointing");

    public static final ConstructingObjectParser<DataFrameTransformStateAndStats, Void> PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_state_and_stats", true,
            a -> new DataFrameTransformStateAndStats((String) a[0], (DataFrameTransformState) a[1], (DataFrameIndexerTransformStats) a[2],
                    (DataFrameTransformCheckpointingInfo) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataFrameTransformState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
                STATS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> DataFrameTransformCheckpointingInfo.fromXContent(p), CHECKPOINTING_INFO_FIELD);
    }

    public static DataFrameTransformStateAndStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String id;
    private final DataFrameTransformState transformState;
    private final DataFrameIndexerTransformStats transformStats;
    private final DataFrameTransformCheckpointingInfo checkpointingInfo;

    public DataFrameTransformStateAndStats(String id, DataFrameTransformState state, DataFrameIndexerTransformStats stats,
                                           DataFrameTransformCheckpointingInfo checkpointingInfo) {
        this.id = id;
        this.transformState = state;
        this.transformStats = stats;
        this.checkpointingInfo = checkpointingInfo;
    }

    public String getId() {
        return id;
    }

    public DataFrameIndexerTransformStats getTransformStats() {
        return transformStats;
    }

    public DataFrameTransformState getTransformState() {
        return transformState;
    }

    public DataFrameTransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, transformState, transformStats, checkpointingInfo);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformStateAndStats that = (DataFrameTransformStateAndStats) other;

        return Objects.equals(this.id, that.id) && Objects.equals(this.transformState, that.transformState)
                && Objects.equals(this.transformStats, that.transformStats)
                && Objects.equals(this.checkpointingInfo, that.checkpointingInfo);
    }
}
