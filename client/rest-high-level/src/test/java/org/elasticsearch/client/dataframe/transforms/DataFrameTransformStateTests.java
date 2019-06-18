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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameTransformStateTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
                DataFrameTransformStateTests::randomDataFrameTransformState,
                DataFrameTransformStateTests::toXContent,
                DataFrameTransformState::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.equals("current_position"))
                .test();
    }

    public static DataFrameTransformState randomDataFrameTransformState() {
        return new DataFrameTransformState(randomFrom(DataFrameTransformTaskState.values()),
            randomFrom(IndexerState.values()),
            randomPositionMap(),
            randomLongBetween(0,10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : DataFrameTransformProgressTests.randomInstance());
    }

    public static void toXContent(DataFrameTransformState state, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("task_state", state.getTaskState().value());
        builder.field("indexer_state", state.getIndexerState().value());
        if (state.getPosition() != null) {
            builder.field("current_position", state.getPosition());
        }
        builder.field("checkpoint", state.getCheckpoint());
        if (state.getReason() != null) {
            builder.field("reason", state.getReason());
        }
        if (state.getProgress() != null) {
            builder.field("progress");
            DataFrameTransformProgressTests.toXContent(state.getProgress(), builder);
        }
        builder.endObject();
    }

    private static Map<String, Object> randomPositionMap() {
        if (randomBoolean()) {
            return null;
        }
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> position = new LinkedHashMap<>();
        for (int i = 0; i < numFields; i++) {
            Object value;
            if (randomBoolean()) {
                value = randomLong();
            } else {
                value = randomAlphaOfLengthBetween(1, 10);
            }
            position.put(randomAlphaOfLengthBetween(3, 10), value);
        }
        return position;
    }
}
