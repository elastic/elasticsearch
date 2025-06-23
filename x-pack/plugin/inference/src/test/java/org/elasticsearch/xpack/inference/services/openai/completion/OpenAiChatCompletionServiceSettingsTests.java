/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<OpenAiChatCompletionServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var modelId = "some model";
        var url = "https://www.elastic.co";
        var org = "organization";
        var maxInputTokens = 8192;

        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    OpenAiServiceFields.ORGANIZATION,
                    org,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new OpenAiChatCompletionServiceSettings(modelId, ServiceUtils.createUri(url), org, maxInputTokens, null))
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly_WithRateLimit() {
        var modelId = "some model";
        var url = "https://www.elastic.co";
        var org = "organization";
        var maxInputTokens = 8192;
        var rateLimit = 2;
        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    OpenAiServiceFields.ORGANIZATION,
                    org,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new OpenAiChatCompletionServiceSettings(modelId, ServiceUtils.createUri(url), org, maxInputTokens, new RateLimitSettings(2)))
        );
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var modelId = "some model";
        var organization = "org";
        var maxInputTokens = 8192;

        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    OpenAiServiceFields.ORGANIZATION,
                    organization,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertNull(serviceSettings.uri());
        assertThat(serviceSettings.modelId(), is(modelId));
        assertThat(serviceSettings.organizationId(), is(organization));
        assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, "", ServiceFields.MODEL_ID, "model")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_MissingOrganization_DoesNotThrowException() {
        var modelId = "some model";
        var maxInputTokens = 8192;

        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId, ServiceFields.MAX_INPUT_TOKENS, maxInputTokens)),
            ConfigurationParseContext.PERSISTENT
        );

        assertNull(serviceSettings.uri());
        assertThat(serviceSettings.modelId(), is(modelId));
        assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
    }

    public void testFromMap_EmptyOrganization_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionServiceSettings.fromMap(
                new HashMap<>(Map.of(OpenAiServiceFields.ORGANIZATION, "", ServiceFields.MODEL_ID, "model")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                org.elasticsearch.common.Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    OpenAiServiceFields.ORGANIZATION
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, url, ServiceFields.MODEL_ID, "model")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]", url, ServiceFields.URL)
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings("model", "url", "org", 1024, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","url":"url","organization_id":"org",""" + """
            "max_input_tokens":1024,"rate_limit":{"requests_per_minute":500}}"""));
    }

    public void testToXContent_WritesAllValues_WithCustomRateLimit() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings("model", "url", "org", 1024, new RateLimitSettings(2));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","url":"url","organization_id":"org",""" + """
            "max_input_tokens":1024,"rate_limit":{"requests_per_minute":2}}"""));
    }

    public void testToXContent_DoesNotWriteOptionalValues() throws IOException {
        URI uri = null;

        var serviceSettings = new OpenAiChatCompletionServiceSettings("model", uri, null, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","rate_limit":{"requests_per_minute":500}}"""));
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionServiceSettings> instanceReader() {
        return OpenAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OpenAiChatCompletionServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected OpenAiChatCompletionServiceSettings mutateInstance(OpenAiChatCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, OpenAiChatCompletionServiceSettingsTests::createRandomWithNonNullUrl);
    }

    private static OpenAiChatCompletionServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    private static OpenAiChatCompletionServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);
        var organizationId = randomFrom(randomAlphaOfLength(15), null);
        var maxInputTokens = randomFrom(randomIntBetween(128, 4096), null);

        return new OpenAiChatCompletionServiceSettings(
            modelId,
            ServiceUtils.createUri(url),
            organizationId,
            maxInputTokens,
            RateLimitSettingsTests.createRandom()
        );
    }

}
