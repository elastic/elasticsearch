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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformStats {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField REASON_FIELD = new ParseField("reason");
    public static final ParseField NODE_FIELD = new ParseField("node");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField CHECKPOINTING_INFO_FIELD = new ParseField("checkpointing");

    public static final ConstructingObjectParser<DataFrameTransformStats, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_state_and_stats_info", true,
        a -> new DataFrameTransformStats((String) a[0], (State) a[1], (String) a[2],
            (NodeAttributes) a[3], (DataFrameIndexerTransformStats) a[4], (DataFrameTransformCheckpointingInfo) a[5]));

    static {
        PARSER.declareString(constructorArg(), ID);
        PARSER.declareField(optionalConstructorArg(), p -> State.fromString(p.text()), STATE_FIELD,
            ObjectParser.ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), REASON_FIELD);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareObject(constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p), STATS_FIELD);
        PARSER.declareObject(optionalConstructorArg(),
            (p, c) -> DataFrameTransformCheckpointingInfo.fromXContent(p), CHECKPOINTING_INFO_FIELD);
    }

    public static DataFrameTransformStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String id;
    private final String reason;
    private final State state;
    private final NodeAttributes node;
    private final DataFrameIndexerTransformStats indexerStats;
    private final DataFrameTransformCheckpointingInfo checkpointingInfo;

    public DataFrameTransformStats(String id, State state, String reason, NodeAttributes node, DataFrameIndexerTransformStats stats,
                                   DataFrameTransformCheckpointingInfo checkpointingInfo) {
        this.id = id;
        this.state = state;
        this.reason = reason;
        this.node = node;
        this.indexerStats = stats;
        this.checkpointingInfo = checkpointingInfo;
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public String getReason() {
        return reason;
    }

    public NodeAttributes getNode() {
        return node;
    }

    public DataFrameIndexerTransformStats getIndexerStats() {
        return indexerStats;
    }

    public DataFrameTransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, reason, node, indexerStats, checkpointingInfo);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformStats that = (DataFrameTransformStats) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.state, that.state)
            && Objects.equals(this.reason, that.reason)
            && Objects.equals(this.node, that.node)
            && Objects.equals(this.indexerStats, that.indexerStats)
            && Objects.equals(this.checkpointingInfo, that.checkpointingInfo);
    }

    public enum State {

        STARTED, INDEXING, ABORTING, STOPPING, STOPPED, FAILED;

        public static State fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
