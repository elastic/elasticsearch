/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings.GOOGLE_VERTEX_AI_CONFIGURABLE_MAX_BATCH_SIZE;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    GoogleVertexAiEmbeddingsServiceSettings> {

    private static final String TEST_LOCATION = "us-central1";
    private static final String INITIAL_TEST_LOCATION = "europe-west1";

    private static final String TEST_PROJECT_ID = "some-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-project-id";

    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    private static final int TEST_MAX_INPUT_TOKENS = 100;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 5;

    private static final int TEST_DIMENSIONS = 64;
    private static final int INITIAL_TEST_DIMENSIONS = 32;

    private static final int TEST_MAX_BATCH_SIZE = 20;
    private static final int INITIAL_TEST_MAX_BATCH_SIZE = 8;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_SIMILARITY_MEASURE = SimilarityMeasure.L2_NORM;
    private static final SimilarityMeasure DEFAULT_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    private static final int TEST_RATE_LIMIT = 10_000;
    private static final int INITIAL_TEST_RATE_LIMIT = 20_000;
    private static final int DEFAULT_RATE_LIMIT = 30_000;

    public void testFromMap_Persistent_WithAllFields_Success() {
        var serviceSettings = GoogleVertexAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_LOCATION,
                TEST_PROJECT_ID,
                TEST_MODEL_ID,
                true,
                TEST_MAX_INPUT_TOKENS,
                TEST_DIMENSIONS,
                TEST_MAX_BATCH_SIZE,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    true,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_DIMENSIONS,
                    TEST_MAX_BATCH_SIZE,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Persistent_OnlyMandatoryFields_UsesDefaultRateLimitAndSimilarity_Success() {
        var serviceSettings = GoogleVertexAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, false, null, null, null, null, null),
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    false,
                    null,
                    null,
                    null,
                    DEFAULT_SIMILARITY_MEASURE,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_WithDimensions_InfersDimensionsSetByUserTrue_Success() {
        var serviceSettings = GoogleVertexAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, null, null, TEST_DIMENSIONS, null, null, null),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    true,
                    null,
                    TEST_DIMENSIONS,
                    null,
                    DEFAULT_SIMILARITY_MEASURE,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_NoDimensions_InfersDimensionsSetByUserFalse_Success() {
        var serviceSettings = GoogleVertexAiEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_LOCATION,
                TEST_PROJECT_ID,
                TEST_MODEL_ID,
                null,
                TEST_MAX_INPUT_TOKENS,
                null,
                TEST_MAX_BATCH_SIZE,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    false,
                    TEST_MAX_INPUT_TOKENS,
                    null,
                    TEST_MAX_BATCH_SIZE,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_MissingLocation_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(null, TEST_PROJECT_ID, TEST_MODEL_ID, true, null, null, null, null, null),
            "[service_settings] does not contain the required setting [location]"
        );
    }

    public void testFromMap_MissingProjectId_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, null, TEST_MODEL_ID, true, null, null, null, null, null),
            "[service_settings] does not contain the required setting [project_id]"
        );
    }

    public void testFromMap_MissingModelId_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, null, true, null, null, null, null, null),
            "[service_settings] does not contain the required setting [model_id]"
        );
    }

    public void testFromMap_NegativeMaxInputTokens_Failure() {
        int maxInputTokens = -1;
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, true, maxInputTokens, null, null, null, null),
            Strings.format("[service_settings] Invalid value [%d]. [max_input_tokens] must be a positive integer", maxInputTokens)
        );
    }

    public void testFromMap_InvalidSimilarity_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, true, null, null, null, "invalid_similarity", null),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(thrownException.validationErrors().getFirst(), is("""
            [service_settings] Invalid value [invalid_similarity] received. \
            [similarity] must be one of [cosine, dot_product, l2_norm]"""));
    }

    public void testFromMap_NegativeDimensions_Failure() {
        int dimensions = -1;
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, true, null, dimensions, null, null, null),
            Strings.format("[service_settings] Invalid value [%d]. [dimensions] must be a positive integer", dimensions)
        );
    }

    public void testFromMap_MaxBatchSizeTooBig_Failure() {
        int tooBigBatchSize = EMBEDDING_MAX_BATCH_SIZE + 1;
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, true, null, null, tooBigBatchSize, null, null),
            Strings.format(
                "[service_settings] Invalid value [%d.0]. [max_batch_size] must be less than or equal to [%d.0]",
                tooBigBatchSize,
                EMBEDDING_MAX_BATCH_SIZE
            )
        );
    }

    public void testFromMap_NegativeMaxBatchSize_Failure() {
        int maxBatchSize = -1;
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, true, null, null, maxBatchSize, null, null),
            Strings.format("[service_settings] Invalid value [%d]. [max_batch_size] must be a positive integer", maxBatchSize)
        );
    }

    public void testFromMap_Request_DimensionsSetByUserProvided_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, true, null, null, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] does not allow the setting [dimensions_set_by_user]")
        );
    }

    public void testFromMap_Persistent_DimensionsSetByUserMissing_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(TEST_LOCATION, TEST_PROJECT_ID, TEST_MODEL_ID, null, null, null, null, null, null),
            "[service_settings] does not contain the required setting [dimensions_set_by_user]"
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_LOCATION,
            TEST_PROJECT_ID,
            TEST_MODEL_ID,
            false,
            TEST_MAX_INPUT_TOKENS,
            TEST_DIMENSIONS,
            TEST_MAX_BATCH_SIZE,
            TEST_SIMILARITY_MEASURE.toString(),
            TEST_RATE_LIMIT
        );
        var originalServiceSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            true,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_BATCH_SIZE,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    INITIAL_TEST_LOCATION,
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_MODEL_ID,
                    true,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_BATCH_SIZE,
                    INITIAL_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            true,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_BATCH_SIZE,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_OnlyMaxBatchSize_UpdatesSuccessfully() {
        var originalServiceSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            true,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_BATCH_SIZE,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(MAX_BATCH_SIZE, TEST_MAX_BATCH_SIZE))
        );
        assertThat(
            updatedServiceSettings,
            is(
                new GoogleVertexAiEmbeddingsServiceSettings(
                    INITIAL_TEST_LOCATION,
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_MODEL_ID,
                    true,
                    INITIAL_TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_BATCH_SIZE,
                    INITIAL_SIMILARITY_MEASURE,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_MaxBatchSizeTooBig_Failure() {
        var originalServiceSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            true,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_BATCH_SIZE,
            INITIAL_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> originalServiceSettings.updateServiceSettings(
                buildServiceSettingsMap(
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    null,
                    null,
                    null,
                    EMBEDDING_MAX_BATCH_SIZE + 1,
                    null,
                    null
                )
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] Invalid value [251.0]. [max_batch_size] must be less than or equal to [250.0]")
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsServiceSettings(
            TEST_LOCATION,
            TEST_PROJECT_ID,
            TEST_MODEL_ID,
            true,
            TEST_MAX_INPUT_TOKENS,
            TEST_DIMENSIONS,
            TEST_MAX_BATCH_SIZE,
            TEST_SIMILARITY_MEASURE,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            equalToIgnoringWhitespaceInJsonString(
                Strings.format(
                    """
                        {
                            "location": "%s",
                            "project_id": "%s",
                            "model_id": "%s",
                            "max_input_tokens": %d,
                            "dimensions": %d,
                            "max_batch_size": %d,
                            "similarity": "%s",
                            "rate_limit": {
                                "requests_per_minute": %d
                            },
                            "dimensions_set_by_user": %b
                        }
                        """,
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_DIMENSIONS,
                    TEST_MAX_BATCH_SIZE,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_RATE_LIMIT,
                    true
                )
            )
        );
    }

    public void testFilteredXContentObject_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsServiceSettings(
            TEST_LOCATION,
            TEST_PROJECT_ID,
            TEST_MODEL_ID,
            true,
            TEST_MAX_INPUT_TOKENS,
            TEST_DIMENSIONS,
            TEST_MAX_BATCH_SIZE,
            TEST_SIMILARITY_MEASURE,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            equalToIgnoringWhitespaceInJsonString(
                Strings.format(
                    """
                        {
                            "location": "%s",
                            "project_id": "%s",
                            "model_id": "%s",
                            "max_input_tokens": %d,
                            "dimensions": %d,
                            "max_batch_size": %d,
                            "similarity": "%s",
                            "rate_limit": {
                                "requests_per_minute": %d
                            }
                        }
                        """,
                    TEST_LOCATION,
                    TEST_PROJECT_ID,
                    TEST_MODEL_ID,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_DIMENSIONS,
                    TEST_MAX_BATCH_SIZE,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_RATE_LIMIT
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiEmbeddingsServiceSettings> instanceReader() {
        return GoogleVertexAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiEmbeddingsServiceSettings mutateInstance(GoogleVertexAiEmbeddingsServiceSettings instance) throws IOException {
        var location = instance.location();
        var projectId = instance.projectId();
        var modelId = instance.modelId();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var maxInputTokens = instance.maxInputTokens();
        var dimensions = instance.dimensions();
        var maxBatchSize = instance.maxBatchSize();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(8)) {
            case 0 -> location = randomValueOtherThan(location, () -> randomAlphaOfLength(10));
            case 1 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(10));
            case 2 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(10));
            case 3 -> dimensionsSetByUser = dimensionsSetByUser == false;
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, ESTestCase::randomNonNegativeIntOrNull);
            case 5 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 6 -> maxBatchSize = randomValueOtherThan(maxBatchSize, () -> randomIntBetween(1, EMBEDDING_MAX_BATCH_SIZE));
            case 7 -> similarity = randomValueOtherThan(similarity, Utils::randomSimilarityMeasure);
            case 8 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new GoogleVertexAiEmbeddingsServiceSettings(
            location,
            projectId,
            modelId,
            dimensionsSetByUser,
            maxInputTokens,
            dimensions,
            maxBatchSize,
            similarity,
            rateLimitSettings
        );
    }

    @Override
    protected GoogleVertexAiEmbeddingsServiceSettings mutateInstanceForVersion(
        GoogleVertexAiEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(GOOGLE_VERTEX_AI_CONFIGURABLE_MAX_BATCH_SIZE)) {
            return new GoogleVertexAiEmbeddingsServiceSettings(
                instance.location(),
                instance.projectId(),
                instance.modelId(),
                instance.dimensionsSetByUser(),
                instance.maxInputTokens(),
                instance.dimensions(),
                instance.maxBatchSize(),
                instance.similarity(),
                instance.rateLimitSettings()
            );
        } else {
            return new GoogleVertexAiEmbeddingsServiceSettings(
                instance.location(),
                instance.projectId(),
                instance.modelId(),
                instance.dimensionsSetByUser(),
                instance.maxInputTokens(),
                instance.dimensions(),
                null,
                instance.similarity(),
                instance.rateLimitSettings()
            );
        }
    }

    private static void assertValidationFailure(Map<String, Object> serviceSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT)
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(thrownException.validationErrors().getFirst(), is(expectedErrorMessage));
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String location,
        @Nullable String projectId,
        @Nullable String modelId,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable Integer maxBatchSize,
        @Nullable String similarity,
        @Nullable Integer rateLimit
    ) {
        HashMap<String, Object> map = new HashMap<>();
        if (location != null) {
            map.put(LOCATION, location);
        }
        if (projectId != null) {
            map.put(PROJECT_ID, projectId);
        }
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (maxInputTokens != null) {
            map.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (dimensions != null) {
            map.put(DIMENSIONS, dimensions);
        }
        if (maxBatchSize != null) {
            map.put(MAX_BATCH_SIZE, maxBatchSize);
        }
        if (similarity != null) {
            map.put(SIMILARITY, similarity);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

    private static GoogleVertexAiEmbeddingsServiceSettings createRandom() {
        return new GoogleVertexAiEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean(),
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            randomBoolean() ? randomIntBetween(1, 250) : null,
            randomFrom(new SimilarityMeasure[] { null, randomSimilarityMeasure() }),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() })
        );
    }
}
