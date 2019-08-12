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
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Predicate;

public class DataFrameTransformCheckpointingInfoTests extends AbstractHlrcXContentTestCase<
        DataFrameTransformCheckpointingInfo,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo> {

    public static DataFrameTransformCheckpointingInfo fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo instance) {
        return new DataFrameTransformCheckpointingInfo(
            DataFrameTransformCheckpointStatsTests.fromHlrc(instance.getLast()),
            DataFrameTransformCheckpointStatsTests.fromHlrc(instance.getNext()),
            instance.getOperationsBehind(),
            instance.getChangesLastDetectedAt());
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo doHlrcParseInstance(XContentParser parser) {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    public DataFrameTransformCheckpointingInfo convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo instance) {
        return fromHlrc(instance);
    }

    public static DataFrameTransformCheckpointingInfo randomDataFrameTransformCheckpointingInfo() {
        return new DataFrameTransformCheckpointingInfo(
            DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
            DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
            randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()));
    }

    @Override
    protected DataFrameTransformCheckpointingInfo createTestInstance() {
        return randomDataFrameTransformCheckpointingInfo();
    }

    @Override
    protected DataFrameTransformCheckpointingInfo doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.contains("position");
    }
}
