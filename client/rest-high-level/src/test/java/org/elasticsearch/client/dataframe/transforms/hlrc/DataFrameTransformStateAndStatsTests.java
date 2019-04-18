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
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;

import java.io.IOException;
import java.util.function.Predicate;

public class DataFrameTransformStateAndStatsTests extends AbstractHlrcXContentTestCase<DataFrameTransformStateAndStats,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats> {

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats.fromXContent(parser);
    }

    @Override
    public DataFrameTransformStateAndStats convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats instance) {
        return new DataFrameTransformStateAndStats(instance.getId(),
                DataFrameTransformStateTests.fromHlrc(instance.getTransformState()),
                DataFrameIndexerTransformStatsTests.fromHlrc(instance.getTransformStats()),
                DataFrameTransformCheckpointingInfoTests.fromHlrc(instance.getCheckpointingInfo()));
    }

    @Override
    protected DataFrameTransformStateAndStats createTestInstance() {
        // the transform id is not part of HLRC as it's only to a field for internal storage, therefore use a default id
        return DataFrameTransformStateTests
                .randomDataFrameTransformStateAndStats(DataFrameIndexerTransformStats.DEFAULT_TRANSFORM_ID);
    }

    @Override
    protected DataFrameTransformStateAndStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStateAndStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("state.current_position");
    }
}

