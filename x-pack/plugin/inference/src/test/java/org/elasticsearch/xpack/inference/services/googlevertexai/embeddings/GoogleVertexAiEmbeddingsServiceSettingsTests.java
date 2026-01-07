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
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.EMBEDDING_MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings.GOOGLE_VERTEX_AI_CONFIGURABLE_MAX_BATCH_SIZE;
import static org.hamcrest.Matchers.containsString;

public class GoogleVertexAiEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    GoogleVertexAiEmbeddingsServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(Map.of());
        var expectedServiceSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            serviceSettingsMap.get(GoogleVertexAiServiceFields.LOCATION).toString(),
            serviceSettingsMap.get(GoogleVertexAiServiceFields.PROJECT_ID).toString(),
            serviceSettingsMap.get(ServiceFields.MODEL_ID).toString(),
            (Boolean) serviceSettingsMap.get(ServiceFields.DIMENSIONS_SET_BY_USER),
            (Integer) serviceSettingsMap.get(ServiceFields.MAX_INPUT_TOKENS),
            (Integer) serviceSettingsMap.get(ServiceFields.DIMENSIONS),
            (Integer) serviceSettingsMap.get(GoogleVertexAiServiceFields.MAX_BATCH_SIZE),
            SimilarityMeasure.fromString((String) serviceSettingsMap.get(ServiceFields.SIMILARITY)),
            null
        );

        var actualServiceSettings = GoogleVertexAiEmbeddingsServiceSettings.fromMap(
            serviceSettingsMap,
            ConfigurationParseContext.PERSISTENT
        );

        assertEquals(expectedServiceSettings, actualServiceSettings);
    }

    public void testFromMap_MissingRequiredStrings_ThrowsValidationException() {
        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(Map.of());
        serviceSettingsMap.put(GoogleVertexAiServiceFields.LOCATION, null);
        serviceSettingsMap.put(GoogleVertexAiServiceFields.PROJECT_ID, null);
        serviceSettingsMap.put(ServiceFields.MODEL_ID, null);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [location]"));
        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [project_id]"));
        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testFromMap_NegativeMaxInputTokens_ThrowsValidationException() {
        var serviceSettingsOverrides = new HashMap<String, Object>();
        serviceSettingsOverrides.put(ServiceFields.MAX_INPUT_TOKENS, randomNegativeInt());

        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(serviceSettingsOverrides);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(exception.getMessage(), containsString("[max_input_tokens] must be a positive integer"));
    }

    public void testFromMap_InvalidSimilarity_ThrowsValidationException() {
        var serviceSettingsOverrides = new HashMap<String, Object>();
        serviceSettingsOverrides.put(ServiceFields.SIMILARITY, "invalid_similarity");

        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(serviceSettingsOverrides);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(exception.getMessage(), containsString("Invalid value [invalid_similarity] received"));
    }

    public void testFromMap_NegativeDimensions_ThrowsValidationException() {
        var serviceSettingsOverrides = new HashMap<String, Object>();
        serviceSettingsOverrides.put(ServiceFields.DIMENSIONS, randomNegativeInt());

        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(serviceSettingsOverrides);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(exception.getMessage(), containsString("[dimensions] must be a positive integer"));
    }

    public void testFromMap_MaxBatchSizeTooBig_ThrowsValidationException() {
        var serviceSettingsOverrides = new HashMap<String, Object>();
        serviceSettingsOverrides.put(GoogleVertexAiServiceFields.MAX_BATCH_SIZE, EMBEDDING_MAX_BATCH_SIZE + 1);

        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(serviceSettingsOverrides);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(exception.getMessage(), containsString("[max_batch_size] must be a less than or equal to [250.0]"));
    }

    public void testFromMap_NegativeMaxBatchSize_ThrowsValidationException() {
        var serviceSettingsOverrides = new HashMap<String, Object>();
        serviceSettingsOverrides.put(GoogleVertexAiServiceFields.MAX_BATCH_SIZE, randomNegativeInt());

        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(serviceSettingsOverrides);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(exception.getMessage(), containsString("[max_batch_size] must be a positive integer"));
    }

    public void testFromMap_RequestConfigurationParseContextAndDimensionsSetByUserProvided_ThrowsValidationException() {
        var serviceSettingsOverrides = new HashMap<String, Object>();
        serviceSettingsOverrides.put(ServiceFields.DIMENSIONS_SET_BY_USER, randomBoolean());

        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(serviceSettingsOverrides);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.REQUEST);
        });

        assertThat(exception.getMessage(), containsString("[service_settings] does not allow the setting [dimensions_set_by_user]"));
    }

    public void testFromMap_PersistentConfigurationParseContextAndDimensionsSetByUserNotProvided_ThrowsValidationException() {
        var serviceSettingsMap = buildServiceSettingsMapWithRandomValues(Map.of());
        serviceSettingsMap.put(ServiceFields.DIMENSIONS_SET_BY_USER, null);
        var exception = expectThrows(ValidationException.class, () -> {
            GoogleVertexAiEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.PERSISTENT);
        });

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] does not contain the required setting [dimensions_set_by_user]")
        );
    }

    private Map<String, Object> buildServiceSettingsMapWithRandomValues(Map<String, Object> defaultOverrides) {
        var serviceSettingsMap = new HashMap<>(defaultOverrides);
        serviceSettingsMap.putIfAbsent(GoogleVertexAiServiceFields.LOCATION, randomAlphaOfLength(8));
        serviceSettingsMap.putIfAbsent(GoogleVertexAiServiceFields.PROJECT_ID, randomAlphaOfLength(8));
        serviceSettingsMap.putIfAbsent(ServiceFields.MODEL_ID, randomAlphaOfLength(8));
        serviceSettingsMap.putIfAbsent(ServiceFields.DIMENSIONS_SET_BY_USER, randomBoolean());
        serviceSettingsMap.putIfAbsent(ServiceFields.MAX_INPUT_TOKENS, randomNonNegativeIntOrNull());
        serviceSettingsMap.putIfAbsent(ServiceFields.DIMENSIONS, randomNonNegativeIntOrNull());
        serviceSettingsMap.putIfAbsent(GoogleVertexAiServiceFields.MAX_BATCH_SIZE, randomIntBetween(1, EMBEDDING_MAX_BATCH_SIZE));
        serviceSettingsMap.putIfAbsent(ServiceFields.SIMILARITY, randomFrom(SimilarityMeasure.values()).toString());
        return serviceSettingsMap;
    }

    public void testUpdateServiceSettings_ValidMaxBatchSize_UpdatesSuccessfully() {
        var initialMaxBatchSize = randomIntBetween(1, EMBEDDING_MAX_BATCH_SIZE);
        var initialSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomBoolean(),
            randomNonNegativeIntOrNull(),
            randomNonNegativeIntOrNull(),
            initialMaxBatchSize,
            randomFrom(new SimilarityMeasure[] { null, randomFrom(SimilarityMeasure.values()) }),
            null
        );

        var updatedMaxBatchSize = randomValueOtherThan(initialMaxBatchSize, () -> randomIntBetween(1, EMBEDDING_MAX_BATCH_SIZE));
        var actualUpdatedSettings = initialSettings.updateServiceSettings(
            Map.of(GoogleVertexAiServiceFields.MAX_BATCH_SIZE, updatedMaxBatchSize)
        );

        var expectedUpdatedSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            initialSettings.location(),
            initialSettings.projectId(),
            initialSettings.modelId(),
            initialSettings.dimensionsSetByUser(),
            initialSettings.maxInputTokens(),
            initialSettings.dimensions(),
            updatedMaxBatchSize,
            initialSettings.similarity(),
            initialSettings.rateLimitSettings()
        );
        assertEquals(expectedUpdatedSettings, actualUpdatedSettings);
    }

    public void testUpdateServiceSettings_InvalidMaxBatchSize_UpdateFails() {
        var initialMaxBatchSize = randomIntBetween(1, EMBEDDING_MAX_BATCH_SIZE);
        var initialSettings = new GoogleVertexAiEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomBoolean(),
            randomNonNegativeIntOrNull(),
            randomNonNegativeIntOrNull(),
            initialMaxBatchSize,
            randomFrom(new SimilarityMeasure[] { null, randomFrom(SimilarityMeasure.values()) }),
            null
        );

        var invalidMaxBatchSize = EMBEDDING_MAX_BATCH_SIZE + 1;
        var exception = expectThrows(ValidationException.class, () -> {
            initialSettings.updateServiceSettings(Map.of(GoogleVertexAiServiceFields.MAX_BATCH_SIZE, invalidMaxBatchSize));
        });

        assertThat(exception.getMessage(), containsString("[max_batch_size] must be a less than or equal to [250.0]"));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsServiceSettings(
            "location",
            "projectId",
            "modelId",
            true,
            10,
            10,
            10,
            SimilarityMeasure.DOT_PRODUCT,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "location": "location",
                "project_id": "projectId",
                "model_id": "modelId",
                "max_input_tokens": 10,
                "dimensions": 10,
                "max_batch_size": 10,
                "similarity": "dot_product",
                "rate_limit": {
                    "requests_per_minute": 30000
                },
                "dimensions_set_by_user": true
            }
            """));
    }

    public void testFilteredXContentObject_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsServiceSettings(
            "location",
            "projectId",
            "modelId",
            true,
            10,
            10,
            10,
            SimilarityMeasure.DOT_PRODUCT,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "location": "location",
                "project_id": "projectId",
                "model_id": "modelId",
                "max_input_tokens": 10,
                "dimensions": 10,
                "max_batch_size": 10,
                "similarity": "dot_product",
                "rate_limit": {
                    "requests_per_minute": 30000
                }
            }
            """));
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

    private static GoogleVertexAiEmbeddingsServiceSettings createRandom() {
        return new GoogleVertexAiEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean(),
            randomNonNegativeIntOrNull(),
            randomNonNegativeIntOrNull(),
            randomNonNegativeIntOrNull(),
            randomFrom(new SimilarityMeasure[] { null, randomSimilarityMeasure() }),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() })
        );
    }

}
