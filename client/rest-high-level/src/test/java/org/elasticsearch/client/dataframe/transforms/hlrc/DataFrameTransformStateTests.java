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

package org.elasticsearch.client.dataframe.transforms.hlrc;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class DataFrameTransformStateTests extends AbstractHlrcXContentTestCase<DataFrameTransformState,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformState> {

    public static DataFrameTransformState fromHlrc(org.elasticsearch.client.dataframe.transforms.DataFrameTransformState instance) {
        return new DataFrameTransformState(DataFrameTransformTaskState.fromString(instance.getTaskState().value()),
                IndexerState.fromString(instance.getIndexerState().value()), instance.getPosition(), instance.getCheckpoint(),
                instance.getReason(), DataFrameTransformProgressTests.fromHlrc(instance.getProgress()));
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformState doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformState.fromXContent(parser);
    }

    @Override
    public DataFrameTransformState convertHlrcToInternal(org.elasticsearch.client.dataframe.transforms.DataFrameTransformState instance) {
        return fromHlrc(instance);
    }

    @Override
    protected DataFrameTransformState createTestInstance() {
        return randomDataFrameTransformState();
    }

    @Override
    protected DataFrameTransformState doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformState.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("current_position");
    }

    public static DataFrameTransformStateAndStats randomDataFrameTransformStateAndStats(String id) {
        return new DataFrameTransformStateAndStats(id,
            randomDataFrameTransformState(),
            randomStats(id),
            randomDataFrameTransformCheckpointingInfo());
    }

    public static DataFrameTransformCheckpointingInfo randomDataFrameTransformCheckpointingInfo() {
        return new DataFrameTransformCheckpointingInfo(randomDataFrameTransformCheckpointStats(),
            randomDataFrameTransformCheckpointStats(), randomNonNegativeLong());
    }

    public static DataFrameTransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new DataFrameTransformCheckpointStats(randomNonNegativeLong(), randomNonNegativeLong());
    }

    public static DataFrameTransformProgress randomDataFrameTransformProgress() {
        long totalDocs = randomNonNegativeLong();
        Long remainingDocs = randomBoolean() ? null : randomLongBetween(0, totalDocs);
        return new DataFrameTransformProgress(totalDocs, remainingDocs);
    }

    public static DataFrameIndexerTransformStats randomStats(String transformId) {
        return new DataFrameIndexerTransformStats(transformId, randomLongBetween(10L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L));
    }

    public static DataFrameTransformState randomDataFrameTransformState() {
        return new DataFrameTransformState(randomFrom(DataFrameTransformTaskState.values()),
            randomFrom(IndexerState.values()),
            randomPosition(),
            randomLongBetween(0,10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomDataFrameTransformProgress());
    }

    private static Map<String, Object> randomPosition() {
        if (randomBoolean()) {
            return null;
        }
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> position = new HashMap<>();
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
