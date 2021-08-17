/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.SettingsConfig;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class SettingsConfigTests extends AbstractResponseTestCase<
    org.elasticsearch.xpack.core.transform.transforms.SettingsConfig,
    SettingsConfig> {

    public static org.elasticsearch.xpack.core.transform.transforms.SettingsConfig randomSettingsConfig() {
        return new org.elasticsearch.xpack.core.transform.transforms.SettingsConfig(
            randomBoolean() ? null : randomIntBetween(10, 10_000),
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : randomIntBetween(0, 1),
            randomBoolean() ? null : randomIntBetween(0, 1)
        );
    }

    public static void assertHlrcEquals(
        org.elasticsearch.xpack.core.transform.transforms.SettingsConfig serverTestInstance,
        SettingsConfig clientInstance
    ) {
        assertEquals(serverTestInstance.getMaxPageSearchSize(), clientInstance.getMaxPageSearchSize());
        assertEquals(serverTestInstance.getDocsPerSecond(), clientInstance.getDocsPerSecond());
        assertEquals(serverTestInstance.getDatesAsEpochMillis(), clientInstance.getDatesAsEpochMillis());
        assertEquals(serverTestInstance.getInterimResults(), clientInstance.getInterimResults());
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.SettingsConfig createServerTestInstance(XContentType xContentType) {
        return randomSettingsConfig();
    }

    @Override
    protected SettingsConfig doParseToClientInstance(XContentParser parser) throws IOException {
        return SettingsConfig.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.transform.transforms.SettingsConfig serverTestInstance,
        SettingsConfig clientInstance
    ) {
        assertHlrcEquals(serverTestInstance, clientInstance);
    }

}
