/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AlibabaCloudSearchEmbeddingsServiceSettings> {

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 1024;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 256;

    private static final String TEST_SERVICE_ID = "test-service-id";
    private static final String INITIAL_TEST_SERVICE_ID = "initial-test-service-id";
    private static final String TEST_HOST = "test-host";
    private static final String INITIAL_TEST_HOST = "initial-test-host";
    private static final String TEST_WORKSPACE_NAME = "test-workspace-name";
    private static final String INITIAL_TEST_WORKSPACE_NAME = "initial-test-workspace-name";
    private static final String TEST_HTTP_SCHEMA = "https";
    private static final String INITIAL_TEST_HTTP_SCHEMA = "http";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static AlibabaCloudSearchEmbeddingsServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchEmbeddingsServiceSettings(
            commonSettings,
            randomFrom(SimilarityMeasure.values()),
            randomInt(TEST_DIMENSIONS),
            randomInt(TEST_MAX_INPUT_TOKENS)
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new AlibabaCloudSearchEmbeddingsServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                INITIAL_TEST_SERVICE_ID,
                INITIAL_TEST_HOST,
                INITIAL_TEST_WORKSPACE_NAME,
                INITIAL_TEST_HTTP_SCHEMA,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            ),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    TEST_SIMILARITY_MEASURE.toString(),
                    ServiceFields.DIMENSIONS,
                    TEST_DIMENSIONS,
                    ServiceFields.MAX_INPUT_TOKENS,
                    TEST_MAX_INPUT_TOKENS,
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        INITIAL_TEST_SERVICE_ID,
                        INITIAL_TEST_HOST,
                        INITIAL_TEST_WORKSPACE_NAME,
                        TEST_HTTP_SCHEMA,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    ),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new AlibabaCloudSearchEmbeddingsServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                INITIAL_TEST_SERVICE_ID,
                INITIAL_TEST_HOST,
                INITIAL_TEST_WORKSPACE_NAME,
                INITIAL_TEST_HTTP_SCHEMA,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            ),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Success() {
        var serviceSettings = AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    TEST_SIMILARITY_MEASURE.toString(),
                    ServiceFields.DIMENSIONS,
                    TEST_DIMENSIONS,
                    ServiceFields.MAX_INPUT_TOKENS,
                    TEST_MAX_INPUT_TOKENS,
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            ),
            null
        );

        assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        TEST_SERVICE_ID,
                        TEST_HOST,
                        TEST_WORKSPACE_NAME,
                        TEST_HTTP_SCHEMA,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    ),
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchEmbeddingsServiceSettings> instanceReader() {
        return AlibabaCloudSearchEmbeddingsServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings mutateInstance(AlibabaCloudSearchEmbeddingsServiceSettings instance)
        throws IOException {
        var commonSettings = instance.getCommonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.getMaxInputTokens();

        switch (between(0, 3)) {
            case 0 -> commonSettings = randomValueOtherThan(
                instance.getCommonSettings(),
                AlibabaCloudSearchServiceSettingsTests::createRandom
            );
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()));
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomIntBetween(32, 256));
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomIntBetween(16, 1024));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AlibabaCloudSearchEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens);

    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        return AlibabaCloudSearchServiceSettingsTests.getServiceSettingsMap(serviceId, host, workspaceName);
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings mutateInstanceForVersion(
        AlibabaCloudSearchEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
