/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<
    AlibabaCloudSearchCompletionServiceSettings> {
    public static AlibabaCloudSearchCompletionServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchCompletionServiceSettings(commonSettings);
    }

    public void testFromMap() {
        var model = "model";
        var host = "host";
        var workspaceName = "default";
        var httpSchema = "https";
        var serviceSettings = AlibabaCloudSearchCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.HOST,
                    host,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    model,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    workspaceName,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    httpSchema
                )
            ),
            null
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchCompletionServiceSettings(
                    new AlibabaCloudSearchServiceSettings(model, host, workspaceName, httpSchema, null)
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
