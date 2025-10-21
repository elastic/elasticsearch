/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<IbmWatsonxChatCompletionServiceSettings> {
    private static final URI TEST_URI = URI.create("abc.com");

    private static IbmWatsonxChatCompletionServiceSettings createRandom() {
        return new IbmWatsonxChatCompletionServiceSettings(
            TEST_URI,
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomFrom(RateLimitSettingsTests.createRandom(), null)
        );
    }

    private IbmWatsonxChatCompletionServiceSettings getServiceSettings(Map<String, String> map) {
        return IbmWatsonxChatCompletionServiceSettings.fromMap(new HashMap<>(map), ConfigurationParseContext.PERSISTENT);
    }

    public void testFromMap_WithAllParameters_CreatesSettingsCorrectly() {
        var model = randomAlphaOfLength(8);
        var projectId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);

        var serviceSettings = getServiceSettings(
            Map.of(
                ServiceFields.URL,
                TEST_URI.toString(),
                IbmWatsonxServiceFields.API_VERSION,
                apiVersion,
                ServiceFields.MODEL_ID,
                model,
                IbmWatsonxServiceFields.PROJECT_ID,
                projectId
            )
        );
        assertThat(serviceSettings, is(new IbmWatsonxChatCompletionServiceSettings(TEST_URI, apiVersion, model, projectId, null)));
    }

    public void testFromMap_Fails_WithoutRequiredParam_Url() {
        var ex = expectThrows(
            ValidationException.class,
            () -> getServiceSettings(
                Map.of(
                    IbmWatsonxServiceFields.API_VERSION,
                    randomAlphaOfLength(8),
                    ServiceFields.MODEL_ID,
                    randomAlphaOfLength(8),
                    IbmWatsonxServiceFields.PROJECT_ID,
                    randomAlphaOfLength(8)
                )
            )
        );
        assertThat(ex.getMessage(), equalTo(generateErrorMessage("url")));
    }

    public void testFromMap_Fails_WithoutRequiredParam_ApiVersion() {
        var ex = expectThrows(
            ValidationException.class,
            () -> getServiceSettings(
                Map.of(
                    ServiceFields.URL,
                    TEST_URI.toString(),
                    ServiceFields.MODEL_ID,
                    randomAlphaOfLength(8),
                    IbmWatsonxServiceFields.PROJECT_ID,
                    randomAlphaOfLength(8)
                )
            )
        );
        assertThat(ex.getMessage(), equalTo(generateErrorMessage("api_version")));
    }

    public void testFromMap_Fails_WithoutRequiredParam_ModelId() {
        var ex = expectThrows(
            ValidationException.class,
            () -> getServiceSettings(
                Map.of(
                    ServiceFields.URL,
                    TEST_URI.toString(),
                    IbmWatsonxServiceFields.API_VERSION,
                    randomAlphaOfLength(8),
                    IbmWatsonxServiceFields.PROJECT_ID,
                    randomAlphaOfLength(8)
                )
            )
        );
        assertThat(ex.getMessage(), equalTo(generateErrorMessage("model_id")));
    }

    public void testFromMap_Fails_WithoutRequiredParam_ProjectId() {
        var ex = expectThrows(
            ValidationException.class,
            () -> getServiceSettings(
                Map.of(
                    ServiceFields.URL,
                    TEST_URI.toString(),
                    IbmWatsonxServiceFields.API_VERSION,
                    randomAlphaOfLength(8),
                    ServiceFields.MODEL_ID,
                    randomAlphaOfLength(8)
                )
            )
        );
        assertThat(ex.getMessage(), equalTo(generateErrorMessage("project_id")));
    }

    private String generateErrorMessage(String field) {
        return "Validation Failed: 1: [service_settings] does not contain the required setting [" + field + "];";
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new IbmWatsonxChatCompletionServiceSettings(TEST_URI, "2024-05-02", "model", "project_id", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "url":"abc.com",
                "api_version":"2024-05-02",
                "model_id":"model",
                "project_id":"project_id",
                "rate_limit": {
                    "requests_per_minute":120
                }
            }"""));
    }

    @Override
    protected Writeable.Reader<IbmWatsonxChatCompletionServiceSettings> instanceReader() {
        return IbmWatsonxChatCompletionServiceSettings::new;
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings mutateInstance(IbmWatsonxChatCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, IbmWatsonxChatCompletionServiceSettingsTests::createRandom);
    }
}
