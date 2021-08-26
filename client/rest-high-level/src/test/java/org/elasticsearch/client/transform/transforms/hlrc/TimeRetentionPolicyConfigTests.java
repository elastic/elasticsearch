/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class TimeRetentionPolicyConfigTests extends AbstractResponseTestCase<
    org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig,
    TimeRetentionPolicyConfig> {

    public static org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig randomTimeSyncConfig() {
        return new org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig(
            randomAlphaOfLengthBetween(1, 10),
            new TimeValue(randomNonNegativeLong())
        );
    }

    public static void assertHlrcEquals(
        org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig serverTestInstance,
        TimeRetentionPolicyConfig clientInstance
    ) {
        assertEquals(serverTestInstance.getField(), clientInstance.getField());
        assertEquals(serverTestInstance.getMaxAge(), clientInstance.getMaxAge());
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig createServerTestInstance(
        XContentType xContentType
    ) {
        return randomTimeSyncConfig();
    }

    @Override
    protected TimeRetentionPolicyConfig doParseToClientInstance(XContentParser parser) throws IOException {
        return TimeRetentionPolicyConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig serverTestInstance,
        TimeRetentionPolicyConfig clientInstance
    ) {
        assertHlrcEquals(serverTestInstance, clientInstance);
    }

}
