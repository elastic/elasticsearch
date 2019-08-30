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

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformCheckpointStatsTests extends AbstractResponseTestCase<
        DataFrameTransformCheckpointStats,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats> {

    public static DataFrameTransformCheckpointStats fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats instance) {
        return new DataFrameTransformCheckpointStats(instance.getCheckpoint(),
            DataFrameIndexerPositionTests.fromHlrc(instance.getPosition()),
            DataFrameTransformProgressTests.fromHlrc(instance.getCheckpointProgress()),
            instance.getTimestampMillis(),
            instance.getTimeUpperBoundMillis());
    }

    public static DataFrameTransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new DataFrameTransformCheckpointStats(randomLongBetween(1, 1_000_000),
            DataFrameIndexerPositionTests.randomDataFrameIndexerPosition(),
            randomBoolean() ? null : DataFrameTransformProgressTests.randomDataFrameTransformProgress(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
    }

    @Override
    protected DataFrameTransformCheckpointStats createServerTestInstance(XContentType xContentType) {
        return randomDataFrameTransformCheckpointStats();
    }

    @Override
    protected org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats doParseToClientInstance(XContentParser parser)
        throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats.fromXContent(parser);
    }

    @Override
    protected void assertInstances(DataFrameTransformCheckpointStats serverTestInstance,
                                   org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats clientInstance) {
        assertThat(serverTestInstance.getCheckpoint(), equalTo(clientInstance.getCheckpoint()));
        assertThat(serverTestInstance.getPosition().getBucketsPosition(), equalTo(clientInstance.getPosition().getBucketsPosition()));
        assertThat(serverTestInstance.getPosition().getIndexerPosition(), equalTo(clientInstance.getPosition().getIndexerPosition()));
        assertThat(serverTestInstance.getTimestampMillis(), equalTo(clientInstance.getTimestampMillis()));
        assertThat(serverTestInstance.getTimeUpperBoundMillis(), equalTo(clientInstance.getTimeUpperBoundMillis()));
        if (serverTestInstance.getCheckpointProgress() != null) {
            assertThat(serverTestInstance.getCheckpointProgress().getDocumentsIndexed(),
                equalTo(clientInstance.getCheckpointProgress().getDocumentsIndexed()));
            assertThat(serverTestInstance.getCheckpointProgress().getDocumentsProcessed(),
                equalTo(clientInstance.getCheckpointProgress().getDocumentsProcessed()));
            assertThat(serverTestInstance.getCheckpointProgress().getPercentComplete(),
                equalTo(clientInstance.getCheckpointProgress().getPercentComplete()));
            assertThat(serverTestInstance.getCheckpointProgress().getTotalDocs(),
                equalTo(clientInstance.getCheckpointProgress().getTotalDocs()));
        }
    }
}
