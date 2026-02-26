/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FireworksAiEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<FireworksAiEmbeddingsServiceSettings> {

    private static final String DEFAULT_URL = "https://api.fireworks.ai/inference/v1/embeddings";

    public static FireworksAiEmbeddingsServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    public static FireworksAiEmbeddingsServiceSettings createRandom() {
        return createRandom(randomAlphaOfLength(15));
    }

    private static FireworksAiEmbeddingsServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        return new FireworksAiEmbeddingsServiceSettings(
            modelId,
            createUri(url),
            similarityMeasure,
            dims,
            maxInputTokens,
            randomBoolean(),
            RateLimitSettingsTests.createRandom()
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    modelId,
                    createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    true,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var maxInputTokens = 512;
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    modelId,
                    createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    null,
                    maxInputTokens,
                    false,
                    null
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    FireworksAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                    false
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    modelId,
                    createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    false,
                    null
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly_WithRateLimitSettings() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var rateLimit = 3;
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    FireworksAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                    false,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    modelId,
                    createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    false,
                    new RateLimitSettings(rateLimit)
                )
            )
        );
    }

    public void testFromMap_Request_UsesDefaultUrl_WhenUrlNotProvided() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "m")),
            ConfigurationParseContext.REQUEST
        );
        assertThat(serviceSettings.uri(), is(URI.create(DEFAULT_URL)));
        assertThat(serviceSettings.modelId(), is("m"));
    }

    public void testFromMap_Request_UsesDefaultRateLimit_WhenRateLimitNotProvided() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "m")),
            ConfigurationParseContext.REQUEST
        );
        assertThat(serviceSettings.rateLimitSettings(), is(FireworksAiEmbeddingsServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS));
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var settings = FireworksAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(FireworksAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, true, ServiceFields.MODEL_ID, "m")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.dimensionsSetByUser(), is(true));
        assertNull(settings.dimensions());
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        var settingsMap = getServiceSettingsMap("model-foo", null, 0, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var dimensions = randomNegativeInt();
        var settingsMap = getServiceSettingsMap("model-foo", null, dimensions, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [dimensions] must be a positive integer;",
                    dimensions
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        var settingsMap = getServiceSettingsMap("model-foo", null, null, 0, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var maxInputTokens = randomNegativeInt();
        var settingsMap = getServiceSettingsMap("model-foo", null, null, maxInputTokens, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer;",
                    maxInputTokens
                )
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, "", ServiceFields.MODEL_ID, "m")),
                ConfigurationParseContext.REQUEST
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

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, url, ServiceFields.MODEL_ID, "m")),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]", url, ServiceFields.URL)
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity, ServiceFields.MODEL_ID, "m")),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. [similarity] "
                    + "must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    public void testFromMap_ThrowsException_WhenModelIdIsMissing() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new FireworksAiEmbeddingsServiceSettings(
            "model",
            URI.create("https://api.fireworks.ai/inference/v1/embeddings"),
            SimilarityMeasure.DOT_PRODUCT,
            1,
            2,
            false,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "model_id": "model",
                "url": "https://api.fireworks.ai/inference/v1/embeddings",
                "similarity": "dot_product",
                "dimensions": 1,
                "max_input_tokens": 2,
                "rate_limit": {
                    "requests_per_minute": 6000
                },
                "dimensions_set_by_user": false
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new FireworksAiEmbeddingsServiceSettings(
            "model",
            URI.create("https://api.fireworks.ai/inference/v1/embeddings"),
            null,
            null,
            null,
            true,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "model_id": "model",
                "url": "https://api.fireworks.ai/inference/v1/embeddings",
                "rate_limit": {
                    "requests_per_minute": 6000
                },
                "dimensions_set_by_user": true
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new FireworksAiEmbeddingsServiceSettings(
            "model",
            URI.create("https://api.fireworks.ai/inference/v1/embeddings"),
            SimilarityMeasure.DOT_PRODUCT,
            1,
            2,
            false,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "model_id": "model",
                "url": "https://api.fireworks.ai/inference/v1/embeddings",
                "similarity": "dot_product",
                "dimensions": 1,
                "max_input_tokens": 2,
                "rate_limit": {
                    "requests_per_minute": 6000
                }
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<FireworksAiEmbeddingsServiceSettings> instanceReader() {
        return FireworksAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected FireworksAiEmbeddingsServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected FireworksAiEmbeddingsServiceSettings mutateInstance(FireworksAiEmbeddingsServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(6)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 2 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure()));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 5 -> dimensionsSetByUser = dimensionsSetByUser == false;
            case 6 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new FireworksAiEmbeddingsServiceSettings(
            modelId,
            uri,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            rateLimitSettings
        );
    }

    public static Map<String, Object> getServiceSettingsMap(
        String model,
        @Nullable String url,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable Boolean dimensionsSetByUser
    ) {
        var map = new HashMap<String, Object>();
        map.put(ServiceFields.MODEL_ID, model);

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }

        if (dimensionsSetByUser != null) {
            map.put(FireworksAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        return map;
    }
}
