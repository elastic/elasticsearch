/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.embeddings;

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
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenShiftAiEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<OpenShiftAiEmbeddingsServiceSettings> {
    private static final String MODEL_VALUE = "some_model";
    private static final String CORRECT_URL_VALUE = "http://www.abc.com";
    private static final String INVALID_URL_VALUE = "^^^";
    private static final int DIMENSIONS_VALUE = 384;
    private static final SimilarityMeasure SIMILARITY_MEASURE_VALUE = SimilarityMeasure.DOT_PRODUCT;
    private static final int MAX_INPUT_TOKENS_VALUE = 128;
    private static final int RATE_LIMIT_VALUE = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                true
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    true
                )
            )
        );
    }

    public void testFromMap_NoModelId_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                null,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    null,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    false
                )
            )
        );
    }

    public void testFromMap_NoUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    null,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [url];")
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    "",
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value empty string. [url] must be a non-empty string;")
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    INVALID_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString(Strings.format("""
            Validation Failed: 1: [service_settings] Invalid url [%s] received for field [url]. \
            Error: unable to parse url [%s]. Reason: Illegal character in path;""", INVALID_URL_VALUE, INVALID_URL_VALUE)));
    }

    public void testFromMap_NoSimilarity_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                null,
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    null,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    false
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    "by_size",
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString("""
            Validation Failed: 1: [service_settings] Invalid value [by_size] received. \
            [similarity] must be one of [cosine, dot_product, l2_norm];"""));
    }

    public void testFromMap_NoDimensions_SetByUserFalse_Persistent_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                null,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    null,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    false
                )
            )
        );
    }

    public void testFromMap_Persistent_WithDimensions_SetByUserFalse_Persistent_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    false
                )
            )
        );
    }

    public void testFromMap_WithDimensions_SetByUserNull_Persistent_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    null
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testFromMap_NoDimensions_SetByUserNull_Request_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                null,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                null
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    null,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    false
                )
            )
        );
    }

    public void testFromMap_WithDimensions_SetByUserNull_Request_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                null
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    true
                )
            )
        );
    }

    public void testFromMap_WithDimensions_SetByUserTrue_Request_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    true
                ),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not allow the setting [dimensions_set_by_user];")
        );
    }

    public void testFromMap_ZeroDimensions_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    0,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_NegativeDimensions_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    -10,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [-10]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_NoInputTokens_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                null,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    null,
                    new RateLimitSettings(RATE_LIMIT_VALUE),
                    false
                )
            )
        );
    }

    public void testFromMap_ZeroInputTokens_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    0,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_NegativeInputTokens_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    -10,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE)),
                    false
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [-10]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_NoRateLimit_Success() {
        var serviceSettings = OpenShiftAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                CORRECT_URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                null,
                false
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenShiftAiEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    CORRECT_URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(3000),
                    false
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new OpenShiftAiEmbeddingsServiceSettings(
            MODEL_VALUE,
            CORRECT_URL_VALUE,
            DIMENSIONS_VALUE,
            SIMILARITY_MEASURE_VALUE,
            MAX_INPUT_TOKENS_VALUE,
            new RateLimitSettings(3),
            false
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": 3
                },
                "dimensions": 384,
                "similarity": "dot_product",
                "max_input_tokens": 128,
                "dimensions_set_by_user": false
            }
            """, MODEL_VALUE, CORRECT_URL_VALUE))));
    }

    @Override
    protected Writeable.Reader<OpenShiftAiEmbeddingsServiceSettings> instanceReader() {
        return OpenShiftAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected OpenShiftAiEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiEmbeddingsServiceSettings mutateInstance(OpenShiftAiEmbeddingsServiceSettings instance) throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        Integer dimensions = instance.dimensions();
        SimilarityMeasure similarity = instance.similarity();
        Integer maxInputTokens = instance.maxInputTokens();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();
        Boolean dimensionsSetByUser = instance.dimensionsSetByUser();

        switch (between(0, 6)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createUri(randomAlphaOfLength(15)));
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomBoolean() ? randomIntBetween(32, 256) : null);
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomBoolean() ? randomFrom(SimilarityMeasure.values()) : null);
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomBoolean() ? randomIntBetween(128, 256) : null);
            case 5 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 6 -> dimensionsSetByUser = randomValueOtherThan(dimensionsSetByUser, ESTestCase::randomBoolean);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new OpenShiftAiEmbeddingsServiceSettings(
            modelId,
            uri,
            dimensions,
            similarity,
            maxInputTokens,
            rateLimitSettings,
            dimensionsSetByUser
        );
    }

    private static OpenShiftAiEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var url = randomAlphaOfLength(15);
        var similarityMeasure = randomBoolean() ? randomFrom(SimilarityMeasure.values()) : null;
        var dimensions = randomBoolean() ? randomIntBetween(32, 256) : null;
        var maxInputTokens = randomBoolean() ? randomIntBetween(128, 256) : null;
        boolean dimensionsSetByUser = randomBoolean();
        return new OpenShiftAiEmbeddingsServiceSettings(
            modelId,
            url,
            dimensions,
            similarityMeasure,
            maxInputTokens,
            RateLimitSettingsTests.createRandom(),
            dimensionsSetByUser
        );
    }

    public static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable HashMap<String, Integer> rateLimitSettings,
        @Nullable Boolean dimensionsSetByUser
    ) {
        HashMap<String, Object> result = new HashMap<>();
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (similarity != null) {
            result.put(ServiceFields.SIMILARITY, similarity);
        }
        if (dimensions != null) {
            result.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            result.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (rateLimitSettings != null) {
            result.put(RateLimitSettings.FIELD_NAME, rateLimitSettings);
        }
        if (dimensionsSetByUser != null) {
            result.put(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        return result;
    }

}
