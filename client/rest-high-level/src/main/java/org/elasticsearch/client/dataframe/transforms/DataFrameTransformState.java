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

import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformState {

    private static final ParseField INDEXER_STATE = new ParseField("indexer_state");
    private static final ParseField TASK_STATE = new ParseField("task_state");

    // 7.3 BWC: current_position only exists in 7.2.  In 7.3+ it is replaced by position.
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField POSITION = new ParseField("position");
    private static final ParseField CHECKPOINT = new ParseField("checkpoint");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField PROGRESS = new ParseField("progress");
    private static final ParseField NODE = new ParseField("node");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataFrameTransformState, Void> PARSER =
            new ConstructingObjectParser<>("data_frame_transform_state", true,
                    args -> {
                        DataFrameTransformTaskState taskState = (DataFrameTransformTaskState) args[0];
                        IndexerState indexerState = (IndexerState) args[1];
                        Map<String, Object> bwcCurrentPosition = (Map<String, Object>) args[2];
                        DataFrameIndexerPosition dataFrameIndexerPosition = (DataFrameIndexerPosition) args[3];

                        // BWC handling, translate current_position to position iff position isn't set
                        if (bwcCurrentPosition != null && dataFrameIndexerPosition == null) {
                            dataFrameIndexerPosition = new DataFrameIndexerPosition(bwcCurrentPosition, null);
                        }

                        long checkpoint = (long) args[4];
                        String reason = (String) args[5];
                        DataFrameTransformProgress progress = (DataFrameTransformProgress) args[6];
                        NodeAttributes node = (NodeAttributes) args[7];

                        return new DataFrameTransformState(taskState, indexerState, dataFrameIndexerPosition, checkpoint, reason, progress, node);
                    });

    static {
        PARSER.declareField(constructorArg(), p -> DataFrameTransformTaskState.fromString(p.text()), TASK_STATE, ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> IndexerState.fromString(p.text()), INDEXER_STATE, ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> p.mapOrdered(), CURRENT_POSITION, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), DataFrameIndexerPosition::fromXContent, POSITION, ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CHECKPOINT);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
        PARSER.declareField(optionalConstructorArg(), DataFrameTransformProgress::fromXContent, PROGRESS, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE, ValueType.OBJECT);
    }

    public static DataFrameTransformState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final DataFrameTransformTaskState taskState;
    private final IndexerState indexerState;
    private final long checkpoint;
    private final DataFrameIndexerPosition position;
    private final String reason;
    private final DataFrameTransformProgress progress;
    private final NodeAttributes node;

    public DataFrameTransformState(DataFrameTransformTaskState taskState,
                                   IndexerState indexerState,
                                   @Nullable DataFrameIndexerPosition position,
                                   long checkpoint,
                                   @Nullable String reason,
                                   @Nullable DataFrameTransformProgress progress,
                                   @Nullable NodeAttributes node) {
        this.taskState = taskState;
        this.indexerState = indexerState;
        this.position = position;
        this.checkpoint = checkpoint;
        this.reason = reason;
        this.progress = progress;
        this.node = node;
    }

    public IndexerState getIndexerState() {
        return indexerState;
    }

    public DataFrameTransformTaskState getTaskState() {
        return taskState;
    }

    @Nullable
    public DataFrameIndexerPosition getPosition() {
        return position;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    @Nullable
    public String getReason() {
        return reason;
    }

    @Nullable
    public DataFrameTransformProgress getProgress() {
        return progress;
    }

    @Nullable
    public NodeAttributes getNode() {
        return node;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformState that = (DataFrameTransformState) other;

        return Objects.equals(this.taskState, that.taskState) &&
            Objects.equals(this.indexerState, that.indexerState) &&
            Objects.equals(this.position, that.position) &&
            Objects.equals(this.progress, that.progress) &&
            this.checkpoint == that.checkpoint &&
            Objects.equals(this.node, that.node) &&
            Objects.equals(this.reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskState, indexerState, position, checkpoint, reason, progress, node);
    }

}
