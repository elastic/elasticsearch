/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.embeddings;

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
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class MistralEmbeddingsServiceSettingsTests extends ESTestCase {
    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var model = "mistral-embed";
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(
            createRequestSettingsMap(model, dims, null, maxInputTokens, SimilarityMeasure.COSINE),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new MistralEmbeddingsServiceSettings(model, dims, true, maxInputTokens, SimilarityMeasure.COSINE, null))
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var model = "mistral-embed";
        var dims = 1536;
        var maxInputTokens = 512;
        var settingsMap = createRequestSettingsMap(model, dims, null, maxInputTokens, SimilarityMeasure.COSINE);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(new MistralEmbeddingsServiceSettings(model, dims, true, maxInputTokens, SimilarityMeasure.COSINE, new RateLimitSettings(3)))
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var model = "mistral-embed";
        var maxInputTokens = 512;
        var settingsMap = createRequestSettingsMap(model, null, null, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(new MistralEmbeddingsServiceSettings(model, null, false, maxInputTokens, SimilarityMeasure.COSINE, null))
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var model = "mistral-embed";
        var maxInputTokens = 512;

        var settingsMap = createRequestSettingsMap(model, null, true, maxInputTokens, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not allow the setting [%s];",
                    MistralConstants.DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var model = "mistral-embed";
        var dims = 1536;
        var maxInputTokens = 512;

        var settingsMap = createRequestSettingsMap(model, dims, false, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(new MistralEmbeddingsServiceSettings(model, dims, false, maxInputTokens, SimilarityMeasure.COSINE, null))
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var model = "mistral-embed";

        var settingsMap = createRequestSettingsMap(model, null, true, null, null);
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new MistralEmbeddingsServiceSettings(model, null, true, null, null, null)));
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var model = "mistral-embed";

        var settingsMap = createRequestSettingsMap(model, null, true, null, SimilarityMeasure.DOT_PRODUCT);
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new MistralEmbeddingsServiceSettings(model, null, true, null, SimilarityMeasure.DOT_PRODUCT, null)));
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var model = "mistral-embed";

        var settingsMap = createRequestSettingsMap(model, 1, null, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new MistralEmbeddingsServiceSettings("model_name", null, true, null, null, new RateLimitSettings(2));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model_name","dimensions_set_by_user":true,"rate_limit":{"requests_per_minute":2}}"""));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new MistralEmbeddingsServiceSettings("model_name", 1024, false, 512, null, new RateLimitSettings(3));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model_name","dimensions":1024,"max_input_tokens":512,""" + """
            "dimensions_set_by_user":false,"rate_limit":{"requests_per_minute":3}}"""));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new MistralEmbeddingsServiceSettings("model_name", 1024, false, 512, null, new RateLimitSettings(3));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model_name","dimensions":1024,"max_input_tokens":512}"""));
    }

    public static HashMap<String, Object> createRequestSettingsMap(
        String model,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        var map = new HashMap<String, Object>(Map.of(MistralConstants.MISTRAL_MODEL_FIELD, model));

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(MistralConstants.DIMENSIONS_SET_BY_USER, dimensionsSetByUser.equals(Boolean.TRUE));
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
