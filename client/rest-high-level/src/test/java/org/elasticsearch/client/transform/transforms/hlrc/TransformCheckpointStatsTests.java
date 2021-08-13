/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
