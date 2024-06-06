/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsServiceSettingsTests extends ESTestCase {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(
            createRequestSettingsMap(target, provider, endpointType, dims, null, maxInputTokens, SimilarityMeasure.COSINE),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    target,
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    dims,
                    true,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";
        var dims = 1536;
        var maxInputTokens = 512;
        var settingsMap = createRequestSettingsMap(target, provider, endpointType, dims, null, maxInputTokens, SimilarityMeasure.COSINE);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    target,
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    dims,
                    true,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    new RateLimitSettings(3)
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";
        var maxInputTokens = 512;
        var settingsMap = createRequestSettingsMap(target, provider, endpointType, null, null, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    target,
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    null,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";
        var maxInputTokens = 512;

        var settingsMap = createRequestSettingsMap(target, provider, endpointType, null, true, maxInputTokens, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not allow the setting [%s];",
                    AzureAiStudioConstants.DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";
        var dims = 1536;
        var maxInputTokens = 512;

        var settingsMap = createRequestSettingsMap(target, provider, endpointType, dims, false, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    target,
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    dims,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";

        var settingsMap = createRequestSettingsMap(target, provider, endpointType, null, true, null, null);
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    target,
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    null,
                    true,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";

        var settingsMap = createRequestSettingsMap(target, provider, endpointType, null, true, null, SimilarityMeasure.DOT_PRODUCT);
        var serviceSettings = AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioEmbeddingsServiceSettings(
                    target,
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    null,
                    true,
                    null,
                    SimilarityMeasure.DOT_PRODUCT,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var target = "http://sometarget.local";
        var provider = "openai";
        var endpointType = "token";

        var settingsMap = createRequestSettingsMap(target, provider, endpointType, 1, null, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new AzureAiStudioEmbeddingsServiceSettings(
            "target_value",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            null,
            true,
            null,
            null,
            new RateLimitSettings(2)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","provider":"openai","endpoint_type":"token",""" + """
            "rate_limit":{"requests_per_minute":2},"dimensions_set_by_user":true}"""));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AzureAiStudioEmbeddingsServiceSettings(
            "target_value",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            1024,
            false,
            512,
            null,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","provider":"openai","endpoint_type":"token",""" + """
            "rate_limit":{"requests_per_minute":3},"dimensions":1024,"max_input_tokens":512,"dimensions_set_by_user":false}"""));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new AzureAiStudioEmbeddingsServiceSettings(
            "target_value",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            1024,
            false,
            512,
            null,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"target_value","provider":"openai","endpoint_type":"token",""" + """
            "rate_limit":{"requests_per_minute":3},"dimensions":1024,"max_input_tokens":512}"""));
    }

    public static HashMap<String, Object> createRequestSettingsMap(
        String target,
        String provider,
        String endpointType,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        var map = new HashMap<String, Object>(
            Map.of(
                AzureAiStudioConstants.TARGET_FIELD,
                target,
                AzureAiStudioConstants.PROVIDER_FIELD,
                provider,
                AzureAiStudioConstants.ENDPOINT_TYPE_FIELD,
                endpointType
            )
        );

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(AzureAiStudioConstants.DIMENSIONS_SET_BY_USER, dimensionsSetByUser.equals(Boolean.TRUE));
        }

        if (maxTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxTokens);
        }

        if (similarityMeasure != null) {
            map.put(SIMILARITY, similarityMeasure.toString());
        }

        return map;
    }
}
