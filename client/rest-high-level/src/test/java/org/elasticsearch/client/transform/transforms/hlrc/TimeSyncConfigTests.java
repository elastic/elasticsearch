/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class TimeSyncConfigTests
        extends AbstractResponseTestCase<org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig, TimeSyncConfig> {

    public static org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig randomTimeSyncConfig() {
        return new org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig(randomAlphaOfLengthBetween(1, 10),
                new TimeValue(randomNonNegativeLong()));
    }

    public static void assertHlrcEquals(org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig serverTestInstance,
            TimeSyncConfig clientInstance) {
        assertEquals(serverTestInstance.getField(), clientInstance.getField());
        assertEquals(serverTestInstance.getDelay(), clientInstance.getDelay());
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig createServerTestInstance(XContentType xContentType) {
        return randomTimeSyncConfig();
    }

    @Override
    protected TimeSyncConfig doParseToClientInstance(XContentParser parser) throws IOException {
        return TimeSyncConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig serverTestInstance,
            TimeSyncConfig clientInstance) {
        assertHlrcEquals(serverTestInstance, clientInstance);
    }

}
