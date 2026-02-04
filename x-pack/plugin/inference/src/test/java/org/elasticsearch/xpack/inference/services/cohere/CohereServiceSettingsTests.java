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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
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
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

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
    private static final CohereServiceSettings.CohereApiVersion TEST_INITIAL_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V1;
    private static final CohereServiceSettings.CohereApiVersion TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V2;

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

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = createSettingsMap(TEST_MODEL_ID, null, TEST_COHERE_API_VERSION);

        var serviceSettings = createInitialCohereServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
            settingsMap,
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(serviceSettings, is(createExpectedCohereServiceSettings(TEST_MODEL_ID, TEST_COHERE_API_VERSION)));
    }

    public void testUpdateServiceSettings_AllFields_OldModelId_Success() {
        HashMap<String, Object> settingsMap = createSettingsMap(null, TEST_LEGACY_MODEL_ID, TEST_COHERE_API_VERSION);

        var serviceSettings = createInitialCohereServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
            settingsMap,
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(serviceSettings, is(createExpectedCohereServiceSettings(TEST_LEGACY_MODEL_ID, TEST_COHERE_API_VERSION)));
    }

    public void testUpdateServiceSettings_AllFields_BothNewAndOldModelIds_Success() {
        HashMap<String, Object> settingsMap = createSettingsMap(TEST_MODEL_ID, TEST_LEGACY_MODEL_ID, TEST_COHERE_API_VERSION);

        var serviceSettings = createInitialCohereServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
            settingsMap,
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(serviceSettings, is(createExpectedCohereServiceSettings(TEST_MODEL_ID, TEST_COHERE_API_VERSION)));
    }

    public void testUpdateServiceSettings_ApiVersionV2_NoModelIds_ThrowsException() {
        HashMap<String, Object> settingsMap = createSettingsMap(null, null, TEST_COHERE_API_VERSION);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> createInitialCohereServiceSettings(null).updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Validation Failed: 1: The [service_settings.model_id] field is required for the Cohere V2 API.;")
        );
    }

    public void testUpdateServiceSettings_ApiVersionV1_NoModelIds_Success() {
        HashMap<String, Object> settingsMap = createSettingsMap(null, null, TEST_INITIAL_COHERE_API_VERSION);

        var serviceSettings = createInitialCohereServiceSettings(null).updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(serviceSettings, is(createExpectedCohereServiceSettings(null, TEST_INITIAL_COHERE_API_VERSION)));
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialCohereServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
            new HashMap<>(),
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(serviceSettings, is(createInitialCohereServiceSettings(INITIAL_TEST_MODEL_ID)));
    }

    private static HashMap<String, Object> createSettingsMap(
        String modelId,
        String oldModelId,
        CohereServiceSettings.CohereApiVersion apiVersion
    ) {
        HashMap<String, Object> resultMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                TEST_URL,
                ServiceFields.SIMILARITY,
                TEST_SIMILARITY_MEASURE.toString(),
                ServiceFields.DIMENSIONS,
                TEST_DIMENSIONS,
                ServiceFields.MAX_INPUT_TOKENS,
                TEST_MAX_INPUT_TOKENS,
                CohereServiceSettings.API_VERSION,
                apiVersion.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );
        if (modelId != null) {
            resultMap.put(ServiceFields.MODEL_ID, modelId);
        }
        if (oldModelId != null) {
            resultMap.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, oldModelId);
        }
        return resultMap;
    }

    private static CohereServiceSettings createInitialCohereServiceSettings(String modelId) {
        return new CohereServiceSettings(
            INITIAL_TEST_URL,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            modelId,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
            TEST_INITIAL_COHERE_API_VERSION
        );
    }

    private static CohereServiceSettings createExpectedCohereServiceSettings(
        String modelId,
        CohereServiceSettings.CohereApiVersion apiVersion
    ) {
        return new CohereServiceSettings(
            TEST_URL,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            modelId,
            new RateLimitSettings(TEST_RATE_LIMIT),
            apiVersion
        );
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CohereServiceSettings.OLD_MODEL_ID_FIELD,
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    null,
                    CohereServiceSettings.CohereApiVersion.V2
                )
            )
        );
    }

    public void testFromMap_WithRateLimit() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CohereServiceSettings.OLD_MODEL_ID_FIELD,
                    model,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    new RateLimitSettings(3),
                    CohereServiceSettings.CohereApiVersion.V2
                )
            )
        );
    }

    public void testFromMap_WhenUsingModelId() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model,
                    CohereServiceSettings.API_VERSION,
                    CohereServiceSettings.CohereApiVersion.V1.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    null,
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testFromMap_MissingModelId() {
        var e = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        1536,
                        ServiceFields.MAX_INPUT_TOKENS,
                        512
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

    public void testFromMap_PrefersModelId_OverModel() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CohereServiceSettings.OLD_MODEL_ID_FIELD,
                    "old_model",
                    ServiceFields.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    null,
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var serviceSettings = CohereServiceSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertNull(serviceSettings.uri());
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
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
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
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
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. [similarity] "
                    + "must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    public void testXContent_WritesModelId() throws IOException {
        var entity = new CohereServiceSettings(
            (String) null,
            null,
            null,
            null,
            "modelId",
            new RateLimitSettings(1),
            CohereServiceSettings.CohereApiVersion.V2
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"modelId","rate_limit":{"requests_per_minute":1},"api_version":"V2"}"""));
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
