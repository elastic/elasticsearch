/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.PERSISTENT;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.REQUEST;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettingsTests.getMapOfCommonEmbeddingSettings;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIEmbeddingServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final JinaAIEmbeddingType TEST_EMBEDDING_TYPE = JinaAIEmbeddingType.BINARY;
    private static final JinaAIEmbeddingType INITIAL_TEST_EMBEDDING_TYPE = JinaAIEmbeddingType.BIT;
    private static final boolean TEST_MULTIMODAL_MODEL = true;
    private static final boolean INITIAL_MULTIMODAL_MODEL = false;

    public static JinaAIEmbeddingServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = randomBoolean() ? null : randomSimilarityMeasure();
        Integer dimensions = randomBoolean() ? null : randomIntBetween(32, 256);
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = JinaAIServiceSettingsTests.createRandom();
        var embeddingType = randomBoolean() ? null : randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();
        var multimodalModel = randomBoolean();

        return new JinaAIEmbeddingServiceSettings(
            commonSettings,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    public static JinaAIEmbeddingServiceSettings createRandomWithNoNullValues() {
        SimilarityMeasure similarityMeasure = randomSimilarityMeasure();
        Integer dimensions = randomIntBetween(32, 256);
        Integer maxInputTokens = randomIntBetween(128, 256);

        var commonSettings = JinaAIServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();
        var multimodalModel = randomBoolean();

        return new JinaAIEmbeddingServiceSettings(
            commonSettings,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    public void testUpdateServiceSettings_DimensionsSetByUserTrue_AllFields_Success() {
        testUpdateServiceSettings_AllFields_Success(true);
    }

    public void testUpdateServiceSettings_DimensionsSetByUserFalse_AllFields_Success() {
        testUpdateServiceSettings_AllFields_Success(false);
    }

    private static void testUpdateServiceSettings_AllFields_Success(boolean dimensionsSetByUser) {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)),
                ServiceFields.SIMILARITY,
                TEST_SIMILARITY_MEASURE.toString(),
                ServiceFields.DIMENSIONS,
                TEST_DIMENSIONS,
                ServiceFields.MAX_INPUT_TOKENS,
                TEST_MAX_INPUT_TOKENS,
                ServiceFields.EMBEDDING_TYPE,
                TEST_EMBEDDING_TYPE.toString(),
                ServiceFields.MULTIMODAL_MODEL,
                TEST_MULTIMODAL_MODEL
            )
        );

        var serviceSettings = createInitialServiceSettings(dimensionsSetByUser, INITIAL_MULTIMODAL_MODEL).updateServiceSettings(
            settingsMap,
            TaskType.EMBEDDING
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingServiceSettings(
                    new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_EMBEDDING_TYPE,
                    true,
                    TEST_MULTIMODAL_MODEL
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DimensionsSetByUserTrue_MultimodalTrue_Success() {
        testUpdateServiceSettings_EmptyMap_Success(true, true);
    }

    public void testUpdateServiceSettings_EmptyMap_DimensionsSetByUserFalse_MultimodalFalse_Success() {
        testUpdateServiceSettings_EmptyMap_Success(false, false);
    }

    private static void testUpdateServiceSettings_EmptyMap_Success(boolean dimensionsSetByUser, boolean multimodalModel) {
        var initialServiceSettings = createInitialServiceSettings(dimensionsSetByUser, multimodalModel);
        var updatedServiceSettings = initialServiceSettings.updateServiceSettings(new HashMap<>(), TaskType.EMBEDDING);

        assertThat(updatedServiceSettings, is(initialServiceSettings));
    }

    private static JinaAIEmbeddingServiceSettings createInitialServiceSettings(boolean dimensionsSetByUser, boolean multimodalModel) {
        return new JinaAIEmbeddingServiceSettings(
            new JinaAIServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    public void testFromMap_persistentContext_createsSettingsCorrectly() {
        testFromMap(randomNonNegativeInt(), PERSISTENT);
    }

    public void testFromMap_requestContext_createsSettingsCorrectly() {
        testFromMap(randomNonNegativeInt(), REQUEST);
    }

    public void testFromMap_requestContext_nullDimensions_createsSettingsCorrectly() {
        testFromMap(null, REQUEST);
    }

    private static void testFromMap(Integer dimensions, ConfigurationParseContext parseContext) {
        var similarity = randomSimilarityMeasure();
        var maxInputTokens = randomNonNegativeInt();
        var model = randomAlphanumericOfLength(8);
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var requestsPerMinute = randomNonNegativeInt();
        var multimodalModel = randomBoolean();
        var settingsMap = getMapOfCommonEmbeddingSettings(
            model,
            similarity,
            dimensions,
            null,
            maxInputTokens,
            embeddingType,
            requestsPerMinute
        );

        settingsMap.put(ServiceFields.MULTIMODAL_MODEL, multimodalModel);

        boolean dimensionsSetByUser;
        if (parseContext == REQUEST) {
            dimensionsSetByUser = dimensions != null;
        } else {
            dimensionsSetByUser = randomBoolean();
            settingsMap.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }

        var serviceSettings = JinaAIEmbeddingServiceSettings.fromMap(settingsMap, parseContext);

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingServiceSettings(
                    new JinaAIServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
                    similarity,
                    dimensions,
                    maxInputTokens,
                    embeddingType,
                    dimensionsSetByUser,
                    multimodalModel
                )
            )
        );
    }

    public void testFromMap_onlyRequiredFields() {
        var serviceSettings = JinaAIEmbeddingServiceSettings.fromMap(
            new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID)),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingServiceSettings(
                    new JinaAIServiceSettings(TEST_MODEL_ID, null),
                    null,
                    null,
                    null,
                    JinaAIEmbeddingType.FLOAT,
                    false,
                    true
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID, ServiceFields.SIMILARITY, similarity)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%s] received. [similarity] "
                        + "must be one of [cosine, dot_product, l2_norm];",
                    similarity
                )
            )
        );
    }

    public void testFromMap_nonPositiveDimensions_throwsError() {
        var dimensions = randomIntBetween(-5, 0);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID, DIMENSIONS, dimensions)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [%s] must be a positive integer;",
                    dimensions,
                    DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError() {
        var embeddingType = "invalid";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID, EMBEDDING_TYPE, embeddingType)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%s] received. [embedding_type] "
                        + "must be one of [binary, bit, float];",
                    embeddingType
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAIEmbeddingServiceSettings(
            new JinaAIServiceSettings(TEST_MODEL_ID, new RateLimitSettings(3)),
            SimilarityMeasure.COSINE,
            5,
            10,
            JinaAIEmbeddingType.FLOAT,
            true,
            true
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
                "model_id":"%s",
                "rate_limit":{"requests_per_minute":3},
                "dimensions":5,
                "embedding_type":"float",
                "max_input_tokens":10,
                "similarity":"cosine",
                "multimodal_model":true,
                "dimensions_set_by_user": true
            }""", TEST_MODEL_ID))));
    }

    public void testUpdate() {
        var settings = createRandom();
        var similarity = randomSimilarityMeasure();
        var dimensions = randomIntBetween(32, 256);

        var newSettings = settings.update(similarity, dimensions);

        var expectedSettings = new JinaAIEmbeddingServiceSettings(
            settings.getCommonSettings(),
            similarity,
            dimensions,
            settings.maxInputTokens(),
            settings.getEmbeddingType(),
            settings.dimensionsSetByUser(),
            settings.isMultimodal()
        );

        assertThat(newSettings, is(expectedSettings));
    }

    @Override
    protected Writeable.Reader<JinaAIEmbeddingServiceSettings> instanceReader() {
        return JinaAIEmbeddingServiceSettings::new;
    }

    @Override
    protected JinaAIEmbeddingServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIEmbeddingServiceSettings mutateInstance(JinaAIEmbeddingServiceSettings instance) throws IOException {
        var commonSettings = instance.getCommonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var embeddingType = instance.getEmbeddingType();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var multimodal = instance.isMultimodal();
        switch (randomInt(6)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, JinaAIServiceSettingsTests::createRandom);
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 2 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 4 -> embeddingType = randomValueOtherThan(embeddingType, () -> randomFrom(JinaAIEmbeddingType.values()));
            case 5 -> dimensionsSetByUser = randomValueOtherThan(dimensionsSetByUser, ESTestCase::randomBoolean);
            case 6 -> multimodal = randomValueOtherThan(multimodal, ESTestCase::randomBoolean);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAIEmbeddingServiceSettings(
            commonSettings,
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser,
            multimodal
        );
    }

    @Override
    protected JinaAIEmbeddingServiceSettings mutateInstanceForVersion(JinaAIEmbeddingServiceSettings instance, TransportVersion version) {
        boolean multimodalModel = instance.isMultimodal();
        boolean dimensionsSetByUser = instance.dimensionsSetByUser();
        JinaAIEmbeddingType embeddingType = instance.getEmbeddingType();

        // default to false for multimodal if node is on a version before embedding task support was added
        if (version.supports(JINA_AI_EMBEDDING_TASK_ADDED) == false) {
            multimodalModel = false;
        }
        // default to false for dimensionsSetByUser if node is on a version before support for setting embedding dimensions was added
        if (version.supports(JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED) == false) {
            dimensionsSetByUser = false;
        }
        // default to null embedding type if node is on a version before embedding type was introduced
        if (version.supports(JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED) == false) {
            embeddingType = null;
        }
        return new JinaAIEmbeddingServiceSettings(
            instance.getCommonSettings(),
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            embeddingType,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(
        String modelName,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        @Nullable Integer requestsPerMinute,
        @Nullable Boolean multimodalModel
    ) {
        var map = getMapOfCommonEmbeddingSettings(
            modelName,
            similarity,
            dimensions,
            dimensionsSetByUser,
            maxInputTokens,
            embeddingType,
            requestsPerMinute
        );

        if (multimodalModel != null) {
            map.put(ServiceFields.MULTIMODAL_MODEL, multimodalModel);
        }
        return map;
    }
}
