/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * {@link CohereServiceSettings} reads a model identifier from either {@link ServiceFields#MODEL_ID} ({@code "model_id"})
 * or the legacy {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"}). When both are present, {@code model_id}
 * wins. {@code testFromMap_*} methods below state which key(s) each scenario uses.
 * <p>
 * Partial updates go through {@link CohereServiceSettings#updateCommonServiceSettings(Map, ValidationException)} (mutable
 * fields: {@code max_input_tokens} and {@code rate_limit}).
 * <p>
 * {@code fromMap} tests that use {@link ConfigurationParseContext#PERSISTENT} are named {@code testFromMap_Persistent_*}
 * (Azure AI Studio service-settings tests use {@code testFromMap_Request_*} where parsing runs in request context).
 */
public class CohereServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereServiceSettings> {

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");
    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String TEST_LEGACY_MODEL_ID = "legacy-test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final CohereServiceSettings.CohereApiVersion INITIAL_TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V1;
    private static final CohereServiceSettings.CohereApiVersion TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V2;
    /** Value under the legacy {@code model} key when testing precedence against {@link ServiceFields#MODEL_ID}. */
    private static final String LEGACY_MODEL_KEY_VALUE = "old_model";
    /** Mirrors {@link CohereServiceSettings#DEFAULT_RATE_LIMIT_SETTINGS} for {@link CohereServiceSettings#fromMap} expectations. */
    private static final RateLimitSettings DEFAULT_COHERE_SERVICE_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);
    private static final String INVALID_TEST_URL = "https://www.abc^.com";

    public static CohereServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    /**
     * The created settings can have a url set to null.
     */
    public static CohereServiceSettings createRandom() {
        return createRandom(randomAlphaOfLengthOrNull(15));
    }

    private static CohereServiceSettings createRandom(String url) {
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var model = randomAlphaOfLengthOrNull(15);

        return new CohereServiceSettings(
            ServiceUtils.createOptionalUri(url),
            similarityMeasure,
            dims,
            maxInputTokens,
            model,
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereServiceSettings.CohereApiVersion.values())
        );
    }

    public void testUpdateCommonServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = createInitialCohereServiceSettings();
        var validationException = new ValidationException();
        var serviceSettings = originalServiceSettings.updateCommonServiceSettings(
            buildServiceSettingsMap(
                TEST_URL,
                TEST_SIMILARITY_MEASURE,
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_MODEL_ID,
                TEST_LEGACY_MODEL_ID,
                TEST_COHERE_API_VERSION.toString(),
                TEST_RATE_LIMIT
            ),
            validationException
        );

        assertTrue(validationException.validationErrors().isEmpty());

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    INITIAL_TEST_URL,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    CohereServiceSettingsTests.INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    CohereServiceSettingsTests.INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    public void testUpdateCommonServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = createInitialCohereServiceSettings();
        var validationException = new ValidationException();
        var updatedServiceSettings = originalServiceSettings.updateCommonServiceSettings(new HashMap<>(), validationException);

        assertTrue(validationException.validationErrors().isEmpty());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    private static CohereServiceSettings createInitialCohereServiceSettings() {
        return new CohereServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            CohereServiceSettingsTests.INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            INITIAL_TEST_COHERE_API_VERSION
        );
    }

    public void testFromMap_Persistent_EmptyMap_CreatesSettingsCorrectly() {
        var serviceSettings = CohereServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    (URI) null,
                    null,
                    null,
                    null,
                    null,
                    DEFAULT_COHERE_SERVICE_RATE_LIMIT_SETTINGS,
                    INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#PERSISTENT}: full map using {@link ServiceFields#MODEL_ID} only (no legacy {@code model} key).
     */
    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereServiceSettings.fromMap(
            buildServiceSettingsMapWithModelIdFieldOnly(
                TEST_URL,
                TEST_SIMILARITY_MEASURE,
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_MODEL_ID,
                TEST_COHERE_API_VERSION.toString(),
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    TEST_URL,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    TEST_COHERE_API_VERSION
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#PERSISTENT}: map contains only the legacy {@link CohereServiceSettings#OLD_MODEL_ID_FIELD}
     * ({@code "model"}) — no {@link ServiceFields#MODEL_ID}. The value is still normalized into {@link CohereServiceSettings#modelId()}.
     */
    public void testFromMap_Persistent_LegacyModelFieldOnly() {
        var serviceSettings = CohereServiceSettings.fromMap(
            buildServiceSettingsMapWithLegacyModelFieldOnly(),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(TEST_URL),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_MODEL_ID,
                    null,
                    INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#REQUEST}: same as {@link #testFromMap_Persistent_LegacyModelFieldOnly} (legacy {@code model} key only),
     * but request parsing defaults the Cohere API version to {@link CohereServiceSettings.CohereApiVersion#V2}.
     */
    public void testFromMap_Request_LegacyModelFieldOnly() {
        var serviceSettings = CohereServiceSettings.fromMap(
            buildServiceSettingsMapWithLegacyModelFieldOnly(),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(TEST_URL),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_MODEL_ID,
                    null,
                    TEST_COHERE_API_VERSION
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#REQUEST}: legacy {@code model} key plus {@link RateLimitSettings}.
     */
    public void testFromMap_Request_LegacyModelFieldWithRateLimit() {
        var serviceSettings = CohereServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_URL,
                INITIAL_TEST_SIMILARITY_MEASURE,
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                null,
                TEST_MODEL_ID,
                null,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(TEST_URL),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT),
                    TEST_COHERE_API_VERSION
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#PERSISTENT}: map uses {@link ServiceFields#MODEL_ID} and explicit {@link CohereServiceSettings#API_VERSION}.
     */
    public void testFromMap_Persistent_ModelIdFieldOnly() {
        var serviceSettings = CohereServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_URL,
                INITIAL_TEST_SIMILARITY_MEASURE,
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_MODEL_ID,
                null,
                INITIAL_TEST_COHERE_API_VERSION.toString(),
                null
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(TEST_URL),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_MODEL_ID,
                    null,
                    INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    public void testFromMap_Request_MissingModelId_ThrowsError() {
        var e = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        TEST_DIMENSIONS,
                        ServiceFields.MAX_INPUT_TOKENS,
                        TEST_MAX_INPUT_TOKENS
                    )
                ),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            e.validationErrors().getFirst(),
            containsString("The [service_settings.model_id] field is required for the Cohere V2 API.")
        );
    }

    /**
     * When both {@link ServiceFields#MODEL_ID} and the legacy {@code model} field are present, the {@code model_id} value wins.
     */
    public void testFromMap_WhenBothModelFieldsPresent_PrefersModelId() {
        var serviceSettings = CohereServiceSettings.fromMap(
            buildServiceSettingsMapWithBothModelFields(INITIAL_TEST_COHERE_API_VERSION.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(TEST_URL),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_MODEL_ID,
                    null,
                    INITIAL_TEST_COHERE_API_VERSION
                )
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")), ConfigurationParseContext.PERSISTENT)
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
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, INVALID_TEST_URL)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]",
                    INVALID_TEST_URL,
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity)),
                ConfigurationParseContext.PERSISTENT
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

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereServiceSettings(
            TEST_URL,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            TEST_COHERE_API_VERSION
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {
                              "url": "%s",
                              "similarity": "%s",
                              "dimensions": %d,
                              "max_input_tokens": %d,
                              "model_id": "%s",
                              "rate_limit": {
                                "requests_per_minute": %d
                              },
                              "api_version": "%s"
                            }
                            """,
                        TEST_URL,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        TEST_RATE_LIMIT,
                        TEST_COHERE_API_VERSION
                    )
                )
            )
        );
    }

    /**
     * Map uses {@link ServiceFields#MODEL_ID} when {@code modelId} is non-null; the legacy
     * {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"}) is omitted.
     */
    private static HashMap<String, Object> buildServiceSettingsMapWithModelIdFieldOnly(
        String url,
        SimilarityMeasure similarity,
        int dimensions,
        int maxInputTokens,
        String modelId,
        String apiVersion,
        int rateLimitRequestsPerMinute
    ) {
        return buildServiceSettingsMap(url, similarity, dimensions, maxInputTokens, modelId, null, apiVersion, rateLimitRequestsPerMinute);
    }

    /**
     * Map uses only the legacy {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"}); {@link ServiceFields#MODEL_ID} is omitted.
     */
    private static HashMap<String, Object> buildServiceSettingsMapWithLegacyModelFieldOnly() {
        return buildServiceSettingsMap(
            TEST_URL,
            INITIAL_TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            null,
            TEST_MODEL_ID,
            null,
            null
        );
    }

    /**
     * Map includes both {@link ServiceFields#MODEL_ID} and the legacy {@code model} key — production code must prefer {@code model_id}.
     */
    private static HashMap<String, Object> buildServiceSettingsMapWithBothModelFields(String apiVersion) {
        return buildServiceSettingsMap(
            TEST_URL,
            INITIAL_TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_MODEL_ID,
            LEGACY_MODEL_KEY_VALUE,
            apiVersion,
            null
        );
    }

    /**
     * @param modelId if non-null, stored under {@link ServiceFields#MODEL_ID}
     * @param legacyModelValue if non-null, stored under {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"})
     */
    private static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String url,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable String modelId,
        @Nullable String legacyModelValue,
        @Nullable String apiVersion,
        @Nullable Integer rateLimitRequestsPerMinute
    ) {
        var result = new HashMap<String, Object>();
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (similarity != null) {
            result.put(ServiceFields.SIMILARITY, similarity.toString());
        }
        if (dimensions != null) {
            result.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            result.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (legacyModelValue != null) {
            result.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, legacyModelValue);
        }
        if (apiVersion != null) {
            result.put(CohereServiceSettings.API_VERSION, apiVersion);
        }
        if (rateLimitRequestsPerMinute != null) {
            result.put(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimitRequestsPerMinute))
            );
        }
        return result;
    }

    @Override
    protected Writeable.Reader<CohereServiceSettings> instanceReader() {
        return CohereServiceSettings::new;
    }

    @Override
    protected CohereServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected CohereServiceSettings mutateInstance(CohereServiceSettings instance) throws IOException {
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();
        switch (randomInt(6)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(15));
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 2 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 4 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(15));
            case 5 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 6 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomFrom(CohereServiceSettings.CohereApiVersion.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereServiceSettings(uriString, similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }

    /**
     * Minimal map for callers that only need {@link ServiceFields#URL} and/or the legacy {@code model} key (no {@code model_id}).
     */
    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        var map = new HashMap<String, Object>();

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (model != null) {
            map.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, model);
        }

        return map;
    }

    @Override
    protected CohereServiceSettings mutateInstanceForVersion(CohereServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereServiceSettings(
                instance.uri(),
                instance.similarity(),
                instance.dimensions(),
                instance.maxInputTokens(),
                instance.modelId(),
                instance.rateLimitSettings(),
                CohereServiceSettings.CohereApiVersion.V1
            );
        }

        return instance;
    }
}
