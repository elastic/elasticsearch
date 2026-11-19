/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AbstractAlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;

import java.io.IOException;
import java.util.Map;

public class AlibabaCloudSearchRerankServiceSettingsTests extends AbstractAlibabaCloudSearchServiceSettingsTests<
    AlibabaCloudSearchRerankServiceSettings> {

    public static AlibabaCloudSearchRerankServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchRerankServiceSettings(commonSettings);
    }

    @Override
    protected AlibabaCloudSearchRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AlibabaCloudSearchRerankServiceSettings.fromMap(map, context);
    }

    @Override
    protected AlibabaCloudSearchRerankServiceSettings createServiceSettings(AlibabaCloudSearchServiceSettings commonSettings) {
        return new AlibabaCloudSearchRerankServiceSettings(commonSettings);
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchRerankServiceSettings> instanceReader() {
        return AlibabaCloudSearchRerankServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchRerankServiceSettings mutateInstance(AlibabaCloudSearchRerankServiceSettings instance) throws IOException {
        return new AlibabaCloudSearchRerankServiceSettings(
            randomValueOtherThan(instance.getCommonSettings(), AlibabaCloudSearchServiceSettingsTests::createRandom)
        );
    }
}
