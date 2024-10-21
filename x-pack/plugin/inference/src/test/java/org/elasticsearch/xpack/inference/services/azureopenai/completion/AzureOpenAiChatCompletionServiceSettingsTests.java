/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AzureOpenAiChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<
    AzureOpenAiChatCompletionServiceSettings> {

    private static AzureOpenAiChatCompletionServiceSettings createRandom() {
        var resourceName = randomAlphaOfLength(8);
        var deploymentId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);

        return new AzureOpenAiChatCompletionServiceSettings(resourceName, deploymentId, apiVersion, null);
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var resourceName = "this-resource";
        var deploymentId = "this-deployment";
        var apiVersion = "2024-01-01";

        var serviceSettings = AzureOpenAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AzureOpenAiServiceFields.RESOURCE_NAME,
                    resourceName,
                    AzureOpenAiServiceFields.DEPLOYMENT_ID,
                    deploymentId,
                    AzureOpenAiServiceFields.API_VERSION,
                    apiVersion
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AzureOpenAiChatCompletionServiceSettings(resourceName, deploymentId, apiVersion, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureOpenAiChatCompletionServiceSettings("resource", "deployment", "2024", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"resource_name":"resource","deployment_id":"deployment","api_version":"2024","rate_limit":{"requests_per_minute":120}}"""));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiChatCompletionServiceSettings> instanceReader() {
        return AzureOpenAiChatCompletionServiceSettings::new;
    }

    @Override
    protected AzureOpenAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiChatCompletionServiceSettings mutateInstance(AzureOpenAiChatCompletionServiceSettings instance)
        throws IOException {
        return randomValueOtherThan(instance, AzureOpenAiChatCompletionServiceSettingsTests::createRandom);
    }
}
