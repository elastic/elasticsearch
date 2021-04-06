/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.client.transform.transforms.hlrc.TransformStatsTests.assertTransformCheckpointInfo;

public class TransformCheckpointingInfoTests extends AbstractResponseTestCase<
    org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo,
    TransformCheckpointingInfo> {

    public static org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo(
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong())
        );
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo createServerTestInstance(
        XContentType xContentType
    ) {
        return randomTransformCheckpointingInfo();
    }

    @Override
    protected TransformCheckpointingInfo doParseToClientInstance(XContentParser parser) throws IOException {
        return TransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo serverTestInstance,
        TransformCheckpointingInfo clientInstance
    ) {
        assertTransformCheckpointInfo(serverTestInstance, clientInstance);
    }
}
