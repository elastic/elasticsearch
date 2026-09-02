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
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class FireworksAiEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<FireworksAiEmbeddingsServiceSettings> {

    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    private static final URI TEST_URI = ServiceUtils.createUri("https://www.test-url.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://www.initial-test-url.com");
    private static final URI DEFAULT_URI = ServiceUtils.createUri("https://api.fireworks.ai/inference/v1/embeddings");

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.L2_NORM;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;

    private static final Integer TEST_DIMENSIONS = 64;
    private static final Integer INITIAL_TEST_DIMENSIONS = 128;

    private static final Integer TEST_MAX_INPUT_TOKENS = 512;
    private static final Integer INITIAL_TEST_MAX_INPUT_TOKENS = 256;

    private static final int TEST_RATE_LIMIT = 1000;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_TEST_RATE_LIMIT = 6000;

    public static FireworksAiEmbeddingsServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    private static FireworksAiEmbeddingsServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);
        SimilarityMeasure similarityMeasure = null;
        Integer dimensions = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = randomBoolean() ? randomSimilarityMeasure() : null;
            dimensions = randomIntBetween(1, 1000);
        }
        var maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        boolean dimensionsSetByUser = randomBoolean();
        return new FireworksAiEmbeddingsServiceSettings(
            modelId,
            createOptionalUri(url),
            similarityMeasure,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            RateLimitSettingsTests.createRandom()
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new FireworksAiEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            false,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                true,
                TEST_RATE_LIMIT,
                TEST_SIMILARITY_MEASURE.toString()
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    false,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new FireworksAiEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            false,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                true,
                TEST_RATE_LIMIT,
                TEST_SIMILARITY_MEASURE.toString()
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    true,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                null,
                TEST_MAX_INPUT_TOKENS,
                null,
                TEST_RATE_LIMIT,
                TEST_SIMILARITY_MEASURE.toString()
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_SIMILARITY_MEASURE,
                    null,
                    TEST_MAX_INPUT_TOKENS,
                    false,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                false,
                TEST_RATE_LIMIT,
                TEST_SIMILARITY_MEASURE.toString()
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new FireworksAiEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    false,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_UsesDefaultUrl_WhenUrlNotProvided() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );
        assertThat(serviceSettings.uri(), is(DEFAULT_URI));
    }

    public void testFromMap_Request_UsesDefaultRateLimit_WhenRateLimitNotProvided() {
        var serviceSettings = FireworksAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );
        assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(DEFAULT_TEST_RATE_LIMIT)));
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var settings = FireworksAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, true, null, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(settings.dimensionsSetByUser(), is(true));
        assertThat(settings.dimensions(), nullValue());
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        int dimensions = 0;
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, null, dimensions, null, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [%d]. [dimensions] must be a positive integer", dimensions))
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var dimensions = randomNegativeInt();
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, null, dimensions, null, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [%d]. [dimensions] must be a positive integer", dimensions))
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        int maxInputTokens = 0;
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, null, null, maxInputTokens, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer", maxInputTokens))
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var maxInputTokens = randomNegativeInt();
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, null, null, maxInputTokens, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer", maxInputTokens))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, "", null, null, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] Invalid value empty string. [url] must be a non-empty string")
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, url, null, null, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(thrownException.validationErrors().getFirst(), is(Strings.format("""
            [service_settings] Invalid url [%s] received for field [url]. \
            Error: unable to parse url [%s]. Reason: Illegal character in authority""", url, url)));
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null, similarity),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] Invalid value [by_size] received. [similarity] must be one of [cosine, dot_product, l2_norm]")
        );
    }

    public void testFromMap_ThrowsException_WhenModelIdIsMissing() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] does not contain the required setting [model_id]")
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new FireworksAiEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            true,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(
            Strings.format(
                """
                    {
                        "model_id": "%s",
                        "url": "%s",
                        "similarity": "%s",
                        "dimensions": %d,
                        "max_input_tokens": %d,
                        "rate_limit": {
                            "requests_per_minute": %d
                        },
                        "dimensions_set_by_user": %b
                    }
                    """,
                TEST_MODEL_ID,
                TEST_URI,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT,
                true
            )
        );
        assertThat(xContentResult, is(expected));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new FireworksAiEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            true,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "similarity": "%s",
                "dimensions": %d,
                "max_input_tokens": %d,
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI, TEST_SIMILARITY_MEASURE.toString(), TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT));
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
            case 2 -> similarity = randomValueOtherThan(similarity, Utils::randomSimilarityMeasure);
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

    public static Map<String, Object> buildServiceSettingsMap(
        String modelId,
        @Nullable String url,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer rateLimit,
        String similarityString
    ) {
        var map = new HashMap<String, Object>();
        map.put(ServiceFields.MODEL_ID, modelId);
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (similarityString != null) {
            map.put(ServiceFields.SIMILARITY, similarityString);
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
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
