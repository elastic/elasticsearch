/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<
    AlibabaCloudSearchCompletionServiceSettings> {

    private static final String MODEL_VALUE = "some model";
    private static final String HOST_VALUE = "some host";
    private static final String WORKSPACE_VALUE = "some workspace";
    private static final String SCHEMA_VALUE = "https";
    private static final long RATE_LIMIT_VALUE = 1L;

    public static AlibabaCloudSearchCompletionServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchCompletionServiceSettings(commonSettings);
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        var serviceSettings = createRandom().updateServiceSettings(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.HOST,
                    HOST_VALUE,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    MODEL_VALUE,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    WORKSPACE_VALUE,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    SCHEMA_VALUE,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                )
            ),
            TaskType.COMPLETION
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchCompletionServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        MODEL_VALUE,
                        HOST_VALUE,
                        WORKSPACE_VALUE,
                        SCHEMA_VALUE,
                        new RateLimitSettings(RATE_LIMIT_VALUE)
                    )
                )
            )
        );
    }

    public void testFromMap() {
        var serviceSettings = AlibabaCloudSearchCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.HOST,
                    HOST_VALUE,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    MODEL_VALUE,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    WORKSPACE_VALUE,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    SCHEMA_VALUE
                )
            ),
            null
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchCompletionServiceSettings(
                    new AlibabaCloudSearchServiceSettings(MODEL_VALUE, HOST_VALUE, WORKSPACE_VALUE, SCHEMA_VALUE, null)
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchCompletionServiceSettings> instanceReader() {
        return AlibabaCloudSearchCompletionServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings mutateInstance(AlibabaCloudSearchCompletionServiceSettings instance)
        throws IOException {
        return createRandom();
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        var map = new HashMap<String, Object>();
        map.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, serviceId);
        map.put(AlibabaCloudSearchServiceSettings.HOST, host);
        map.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, workspaceName);
        return map;
    }
}
