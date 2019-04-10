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
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;

import java.io.IOException;

public class DataFrameTransformCheckpointStatsTests extends AbstractHlrcXContentTestCase<
        DataFrameTransformCheckpointStats,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats> {

    public static DataFrameTransformCheckpointStats fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats instance) {
        return new DataFrameTransformCheckpointStats(instance.getTimestampMillis(), instance.getTimeUpperBoundMillis());
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats.fromXContent(parser);
    }

    @Override
    public DataFrameTransformCheckpointStats convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats instance) {
        return fromHlrc(instance);
    }

    @Override
    protected DataFrameTransformCheckpointStats createTestInstance() {
        return DataFrameTransformStateTests.randomDataFrameTransformCheckpointStats();
    }

    @Override
    protected DataFrameTransformCheckpointStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}
