/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<IbmWatsonxEmbeddingsServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-test-project-id";
    private static final URI TEST_URI = ServiceUtils.createUri("https://test-uri.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://initial-test-uri.com");
    private static final String TEST_API_VERSION = "test-api-version";
    private static final String INITIAL_TEST_API_VERSION = "initial-test-api-version";
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static IbmWatsonxEmbeddingsServiceSettings createRandom() {
        return new IbmWatsonxEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            TEST_URI,
            randomAlphaOfLength(8),
            randomNonNegativeIntOrNull(),
            randomNonNegativeIntOrNull(),
            randomFrom(randomSimilarityMeasure(), null),
            randomFrom(RateLimitSettingsTests.createRandom(), null)
        );
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                IbmWatsonxServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                ServiceFields.URL,
                TEST_URI.toString(),
                IbmWatsonxServiceFields.API_VERSION,
                TEST_API_VERSION,
                ServiceFields.MAX_INPUT_TOKENS,
                TEST_MAX_INPUT_TOKENS,
                ServiceFields.DIMENSIONS,
                TEST_DIMENSIONS,
                ServiceFields.SIMILARITY,
                TEST_SIMILARITY_MEASURE.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING);

        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    TEST_URI,
                    TEST_API_VERSION,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var initialServiceSettings = createInitialServiceSettings();
        var updatedServiceSettings = initialServiceSettings.updateServiceSettings(new HashMap<>(), TaskType.TEXT_EMBEDDING);

        assertThat(updatedServiceSettings, is(initialServiceSettings));
    }

    private static IbmWatsonxEmbeddingsServiceSettings createInitialServiceSettings() {
        return new IbmWatsonxEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var model = randomAlphaOfLength(8);
        var projectId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);
        var maxInputTokens = randomIntBetween(1, 1024);
        var dims = randomIntBetween(1, 10000);
        var similarity = randomSimilarityMeasure();
        var rateLimitSettings = RateLimitSettingsTests.createRandom();

        var serviceSettings = IbmWatsonxEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    IbmWatsonxServiceFields.PROJECT_ID,
                    projectId,
                    ServiceFields.URL,
                    TEST_URI.toString(),
                    IbmWatsonxServiceFields.API_VERSION,
                    apiVersion,
                    ServiceFields.MODEL_ID,
                    model,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.SIMILARITY,
                    similarity.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxEmbeddingsServiceSettings(
                    model,
                    projectId,
                    TEST_URI,
                    apiVersion,
                    maxInputTokens,
                    dims,
                    similarity,
                    rateLimitSettings
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new IbmWatsonxEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_PROJECT_ID,
            TEST_URI,
            TEST_API_VERSION,
            TEST_MAX_INPUT_TOKENS,
            TEST_DIMENSIONS,
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
                            "model_id":"%s",
                            "project_id":"%s",
                            "url":"%s",
                            "api_version":"%s",
                            "max_input_tokens": %d,
                            "dimensions": %d,
                            "similarity": "%s",
                            "rate_limit": {
                                "requests_per_minute":%d
                            }
                        }""",
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    TEST_URI,
                    TEST_API_VERSION,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE,
                    TEST_RATE_LIMIT
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<IbmWatsonxEmbeddingsServiceSettings> instanceReader() {
        return IbmWatsonxEmbeddingsServiceSettings::new;
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings mutateInstance(IbmWatsonxEmbeddingsServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var projectId = instance.projectId();
        var url = instance.url();
        var apiVersion = instance.apiVersion();
        var maxInputTokens = instance.maxInputTokens();
        var dimensions = instance.dimensions();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(7)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(8));
            case 2 -> url = randomValueOtherThan(url, () -> createUri(randomAlphaOfLength(8)));
            case 3 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, ESTestCase::randomNonNegativeIntOrNull);
            case 5 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 6 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new IbmWatsonxEmbeddingsServiceSettings(
            modelId,
            projectId,
            url,
            apiVersion,
            maxInputTokens,
            dimensions,
            similarity,
            rateLimitSettings
        );
    }
}
