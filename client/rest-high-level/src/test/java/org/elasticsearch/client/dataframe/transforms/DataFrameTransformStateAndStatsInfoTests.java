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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameTransformStateAndStatsInfoTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DataFrameTransformStateAndStatsInfoTests::randomInstance,
            DataFrameTransformStateAndStatsInfoTests::toXContent,
            DataFrameTransformStateAndStatsInfo::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.equals("node.attributes") || field.contains("position"))
                .test();
    }

    public static DataFrameTransformStateAndStatsInfo randomInstance() {
        return new DataFrameTransformStateAndStatsInfo(randomAlphaOfLength(10),
            randomBoolean() ? null : randomFrom(DataFrameTransformTaskState.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributesTests.createRandom(),
            DataFrameIndexerTransformStatsTests.randomStats(),
            randomBoolean() ? null : DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    public static void toXContent(DataFrameTransformStateAndStatsInfo stateAndStatsInfo, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(DataFrameTransformStateAndStatsInfo.ID.getPreferredName(), stateAndStatsInfo.getId());
        if (stateAndStatsInfo.getTaskState() != null) {
            builder.field(DataFrameTransformStateAndStatsInfo.TASK_STATE_FIELD.getPreferredName(),
                stateAndStatsInfo.getTaskState().value());
        }
        if (stateAndStatsInfo.getReason() != null) {
            builder.field(DataFrameTransformStateAndStatsInfo.REASON_FIELD.getPreferredName(), stateAndStatsInfo.getReason());
        }
        if (stateAndStatsInfo.getNode() != null) {
            builder.field(DataFrameTransformStateAndStatsInfo.NODE_FIELD.getPreferredName());
            stateAndStatsInfo.getNode().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.field(DataFrameTransformStateAndStatsInfo.STATS_FIELD.getPreferredName());
        DataFrameIndexerTransformStatsTests.toXContent(stateAndStatsInfo.getTransformStats(), builder);
        if (stateAndStatsInfo.getCheckpointingInfo() != null) {
            builder.field(DataFrameTransformStateAndStatsInfo.CHECKPOINTING_INFO_FIELD.getPreferredName());
            DataFrameTransformCheckpointingInfoTests.toXContent(stateAndStatsInfo.getCheckpointingInfo(), builder);
        }
        builder.endObject();
    }
}
