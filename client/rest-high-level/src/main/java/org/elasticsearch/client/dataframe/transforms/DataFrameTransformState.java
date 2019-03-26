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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformState {

    private static final ParseField STATE = new ParseField("transform_state");
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField GENERATION = new ParseField("generation");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataFrameTransformState, Void> PARSER =
            new ConstructingObjectParser<>("data_frame_transform_state",
                    args -> new DataFrameTransformState((IndexerState) args[0], (HashMap<String, Object>) args[1], (long) args[2]));

    static {
        PARSER.declareField(constructorArg(), p -> IndexerState.fromString(p.text()), STATE, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return p.map();
            }
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, CURRENT_POSITION, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), GENERATION);
    }

    public static DataFrameTransformState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final IndexerState state;
    private final long generation;
    private final SortedMap<String, Object> currentPosition;

    public DataFrameTransformState(IndexerState state, @Nullable Map<String, Object> position, long generation) {
        this.state = state;
        this.currentPosition = position == null ? null : Collections.unmodifiableSortedMap(new TreeMap<>(position));
        this.generation = generation;
    }

    public IndexerState getIndexerState() {
        return state;
    }

    @Nullable
    public Map<String, Object> getPosition() {
        return currentPosition;
    }

    public long getGeneration() {
        return generation;
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

        return Objects.equals(this.state, that.state) && Objects.equals(this.currentPosition, that.currentPosition)
                && this.generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, currentPosition, generation);
    }
}
