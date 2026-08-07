/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AbstractAlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsServiceSettingsTests extends AbstractAlibabaCloudSearchServiceSettingsTests<
    AlibabaCloudSearchEmbeddingsServiceSettings> {

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 1024;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 256;

    public static AlibabaCloudSearchEmbeddingsServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchEmbeddingsServiceSettings(
            commonSettings,
            randomFrom(SimilarityMeasure.values()),
            randomInt(TEST_DIMENSIONS),
            randomInt(TEST_MAX_INPUT_TOKENS)
        );
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(map, context);
    }

    @Override
    protected AlibabaCloudSearchEmbeddingsServiceSettings createServiceSettings(AlibabaCloudSearchServiceSettings commonSettings) {
        return new AlibabaCloudSearchEmbeddingsServiceSettings(commonSettings, null, null, null);
    }

    @Override
    protected List<String> additionalImmutableFields() {
        return List.of(ServiceFields.SIMILARITY, ServiceFields.DIMENSIONS);
    }

    public void testFromMap_TaskFields_Success() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, TEST_HTTP_SCHEMA, TEST_RATE_LIMIT);
        map.put(ServiceFields.SIMILARITY, TEST_SIMILARITY_MEASURE.toString());
        map.put(ServiceFields.DIMENSIONS, TEST_DIMENSIONS);
        map.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);

        var serviceSettings = AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()));

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

    public void testFromMap_NonPositiveDimensions_ThrowsException() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null);
        map.put(ServiceFields.DIMENSIONS, randomIntBetween(-10, 0));

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.getCause().getMessage(), containsString("must be a positive integer"));
    }

    public void testFromMap_NonPositiveMaxInputTokens_ThrowsException() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null);
        map.put(ServiceFields.MAX_INPUT_TOKENS, randomIntBetween(-10, 0));

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.getCause().getMessage(), containsString("must be a positive integer"));
    }

    public void testUpdateServiceSettings_MaxInputTokens_IsUpdated() {
        var originalServiceSettings = new AlibabaCloudSearchEmbeddingsServiceSettings(
            initialCommonServiceSettings(),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS))
        );

        assertThat(
            updatedServiceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    initialCommonServiceSettings(),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testUpdateServiceSettings_ExplicitNullMaxInputTokens_ClearsValue() {
        var originalServiceSettings = new AlibabaCloudSearchEmbeddingsServiceSettings(
            initialCommonServiceSettings(),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS
        );

        var update = new HashMap<String, Object>();
        update.put(ServiceFields.MAX_INPUT_TOKENS, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(update);

        assertThat(
            updatedServiceSettings,
            is(
                new AlibabaCloudSearchEmbeddingsServiceSettings(
                    initialCommonServiceSettings(),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    null
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
}
