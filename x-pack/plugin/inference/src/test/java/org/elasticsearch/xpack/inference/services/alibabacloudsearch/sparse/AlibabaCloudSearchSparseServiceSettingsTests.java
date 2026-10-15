/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AbstractAlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;

import java.io.IOException;
import java.util.Map;

public class AlibabaCloudSearchSparseServiceSettingsTests extends AbstractAlibabaCloudSearchServiceSettingsTests<
    AlibabaCloudSearchSparseServiceSettings> {

    public static AlibabaCloudSearchSparseServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchSparseServiceSettings(commonSettings);
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AlibabaCloudSearchSparseServiceSettings.fromMap(map, context);
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings createServiceSettings(AlibabaCloudSearchServiceSettings commonSettings) {
        return new AlibabaCloudSearchSparseServiceSettings(commonSettings);
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchSparseServiceSettings> instanceReader() {
        return AlibabaCloudSearchSparseServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings mutateInstance(AlibabaCloudSearchSparseServiceSettings instance) throws IOException {
        return new AlibabaCloudSearchSparseServiceSettings(
            randomValueOtherThan(instance.getCommonSettings(), AlibabaCloudSearchServiceSettingsTests::createRandom)
        );
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        return AlibabaCloudSearchServiceSettingsTests.getServiceSettingsMap(serviceId, host, workspaceName);
    }
}
