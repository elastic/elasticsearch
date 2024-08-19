/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchSparseServiceSettingsTests extends AbstractWireSerializingTestCase<AlibabaCloudSearchSparseServiceSettings> {
    public static AlibabaCloudSearchSparseServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchSparseServiceSettings(commonSettings);
    }

    public static AlibabaCloudSearchSparseServiceSettings createRandom(String url) {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom(url);
        return new AlibabaCloudSearchSparseServiceSettings(commonSettings);
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var model = "model";
        var host = "host";
        var workspaceName = "default";
        var httpSchema = "https";
        var serviceSettings = AlibabaCloudSearchSparseServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
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
                new AlibabaCloudSearchSparseServiceSettings(
                    new AlibabaCloudSearchServiceSettings(ServiceUtils.createUri(url), model, host, workspaceName, httpSchema, null)
                )
            )
        );
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
        return null;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, String serviceId, String host, String workspaceName) {
        return AlibabaCloudSearchServiceSettingsTests.getServiceSettingsMap(url, serviceId, host, workspaceName);
    }
}
