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

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TransformCheckpointStatsTests extends AbstractResponseTestCase<
        TransformCheckpointStats,
        org.elasticsearch.client.transform.transforms.TransformCheckpointStats> {

    public static TransformCheckpointStats fromHlrc(
            org.elasticsearch.client.transform.transforms.TransformCheckpointStats instance) {
        return new TransformCheckpointStats(instance.getCheckpoint(),
            TransformIndexerPositionTests.fromHlrc(instance.getPosition()),
            TransformProgressTests.fromHlrc(instance.getCheckpointProgress()),
            instance.getTimestampMillis(),
            instance.getTimeUpperBoundMillis());
    }

    public static TransformCheckpointStats randomTransformCheckpointStats() {
        return new TransformCheckpointStats(randomLongBetween(1, 1_000_000),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomBoolean() ? null : TransformProgressTests.randomTransformProgress(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
    }

    @Override
    protected TransformCheckpointStats createServerTestInstance(XContentType xContentType) {
        return randomTransformCheckpointStats();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.TransformCheckpointStats doParseToClientInstance(XContentParser parser)
        throws IOException {
        return org.elasticsearch.client.transform.transforms.TransformCheckpointStats.fromXContent(parser);
    }

    @Override
    protected void assertInstances(TransformCheckpointStats serverTestInstance,
                                   org.elasticsearch.client.transform.transforms.TransformCheckpointStats clientInstance) {
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
