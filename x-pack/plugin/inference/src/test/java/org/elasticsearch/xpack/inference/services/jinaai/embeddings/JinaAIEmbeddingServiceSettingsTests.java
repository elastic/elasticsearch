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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.PERSISTENT;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.REQUEST;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettings.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIEmbeddingServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model";

    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;

    private static final int TEST_DIMENSIONS = 128;
    private static final int INITIAL_TEST_DIMENSIONS = 64;

    private static final int TEST_MAX_INPUT_TOKENS = 256;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 64;

    private static final JinaAIEmbeddingType TEST_EMBEDDING_TYPE = JinaAIEmbeddingType.FLOAT;
    private static final JinaAIEmbeddingType INITIAL_TEST_EMBEDDING_TYPE = JinaAIEmbeddingType.BINARY;

    private static final boolean TEST_DIMENSIONS_SET_BY_USER = true;
    private static final boolean INITIAL_TEST_DIMENSIONS_SET_BY_USER = false;

    private static final boolean TEST_MULTIMODAL_MODEL = true;
    private static final boolean INITIAL_TEST_MULTIMODAL_MODEL = false;

    private static final int TEST_RATE_LIMIT = 500;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 2000;

    public static JinaAIEmbeddingServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = randomBoolean() ? null : randomSimilarityMeasure();
        Integer dimensions = randomBoolean() ? null : randomIntBetween(32, 256);
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = JinaAICommonServiceSettingsTests.createRandom();
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

        var commonSettings = JinaAICommonServiceSettingsTests.createRandom();
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

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        testFromMap(randomNonNegativeInt(), PERSISTENT);
    }

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        testFromMap(randomNonNegativeInt(), REQUEST);
    }

    public void testFromMap_Request_NoDimensions_CreatesSettingsCorrectly() {
        testFromMap(null, REQUEST);
    }

    private static void testFromMap(Integer dimensions, ConfigurationParseContext context) {
        var similarity = randomSimilarityMeasure();
        var maxInputTokens = randomNonNegativeInt();
        var model = randomAlphanumericOfLength(8);
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var requestsPerMinute = randomNonNegativeInt();
        var multimodalModel = randomBoolean();
        var settingsMap = buildServiceSettingsMap(
            model,
            similarity,
            dimensions,
            null,
            maxInputTokens,
            embeddingType,
            requestsPerMinute,
            multimodalModel
        );

        boolean dimensionsSetByUser;
        if (context == REQUEST) {
            dimensionsSetByUser = dimensions != null;
        } else {
            dimensionsSetByUser = randomBoolean();
            settingsMap.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }

        var serviceSettings = JinaAIEmbeddingServiceSettings.fromMap(settingsMap, context);

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
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

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var settingsMap = new HashMap<String, Object>(Map.of(MODEL_ID, TEST_MODEL_ID));
        var serviceSettings = JinaAIEmbeddingServiceSettings.fromMap(settingsMap, randomFrom(ConfigurationParseContext.values()));

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)),
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

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", MODEL_ID))
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, "model", ServiceFields.SIMILARITY, similarity)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%s] received. [similarity] must be one of [cosine, dot_product, l2_norm]",
                    similarity
                )
            )
        );
    }

    public void testFromMap_NonPositiveDimensions_ThrowsError() {
        var dimensions = randomIntBetween(-5, 0);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", DIMENSIONS, dimensions)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [%d]. [%s] must be a positive integer", dimensions, DIMENSIONS))
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError() {
        var embeddingType = "invalid";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, "model", EMBEDDING_TYPE, embeddingType)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%s] received. [embedding_type] must be one of [binary, bit, float]",
                    embeddingType
                )
            )
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_MODEL_ID,
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_DIMENSIONS_SET_BY_USER,
            TEST_MAX_INPUT_TOKENS,
            TEST_EMBEDDING_TYPE,
            TEST_RATE_LIMIT,
            TEST_MULTIMODAL_MODEL
        );
        var originalServiceSettings = new JinaAIEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MULTIMODAL_MODEL
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new JinaAIEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
                    INITIAL_TEST_SIMILARITY,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_EMBEDDING_TYPE,
                    INITIAL_TEST_DIMENSIONS_SET_BY_USER,
                    INITIAL_TEST_MULTIMODAL_MODEL
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new JinaAIEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER,
            INITIAL_TEST_MULTIMODAL_MODEL
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAIEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_EMBEDDING_TYPE,
            TEST_DIMENSIONS_SET_BY_USER,
            TEST_MULTIMODAL_MODEL
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(
            xContentResult,
            equalToIgnoringWhitespaceInJsonString(
                Strings.format(
                    """
                        {
                            "model_id": "%s",
                            "rate_limit": {
                                "requests_per_minute": %d
                            },
                            "dimensions": %d,
                            "embedding_type": "%s",
                            "max_input_tokens": %d,
                            "similarity": "%s",
                            "multimodal_model": %b,
                            "dimensions_set_by_user": %b
                        }
                        """,
                    TEST_MODEL_ID,
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS,
                    TEST_EMBEDDING_TYPE,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    TEST_MULTIMODAL_MODEL,
                    TEST_DIMENSIONS_SET_BY_USER
                )
            )
        );
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
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, JinaAICommonServiceSettingsTests::createRandom);
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

    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        @Nullable Integer rateLimit,
        @Nullable Boolean multimodalModel
    ) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (similarity != null) {
            map.put(SIMILARITY, similarity.toString());
        }
        if (dimensions != null) {
            map.put(DIMENSIONS, dimensions);
        }
        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (maxInputTokens != null) {
            map.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (embeddingType != null) {
            map.put(EMBEDDING_TYPE, embeddingType.toString());
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        if (multimodalModel != null) {
            map.put(MULTIMODAL_MODEL, multimodalModel);
        }
        return map;
    }
}
