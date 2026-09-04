/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AbstractAlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AlibabaCloudSearchCompletionServiceSettingsTests extends AbstractAlibabaCloudSearchServiceSettingsTests<
    AlibabaCloudSearchCompletionServiceSettings> {

    public static AlibabaCloudSearchCompletionServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchCompletionServiceSettings(commonSettings);
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AlibabaCloudSearchCompletionServiceSettings.fromMap(map, context);
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings createServiceSettings(AlibabaCloudSearchServiceSettings commonSettings) {
        return new AlibabaCloudSearchCompletionServiceSettings(commonSettings);
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
        return new AlibabaCloudSearchCompletionServiceSettings(
            randomValueOtherThan(instance.getCommonSettings(), AlibabaCloudSearchServiceSettingsTests::createRandom)
        );
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        var map = new HashMap<String, Object>();
        map.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, serviceId);
        map.put(AlibabaCloudSearchServiceSettings.HOST, host);
        map.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, workspaceName);
        return map;
    }
}
