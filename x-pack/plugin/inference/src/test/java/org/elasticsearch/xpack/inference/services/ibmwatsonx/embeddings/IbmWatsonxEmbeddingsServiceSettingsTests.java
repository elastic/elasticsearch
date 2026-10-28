/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.AbstractIbmWatsonxServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxEmbeddingsServiceSettingsTests extends AbstractIbmWatsonxServiceSettingsTests<IbmWatsonxEmbeddingsServiceSettings> {

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 256;

    private static final int TEST_DIMENSIONS = 64;
    private static final int INITIAL_TEST_DIMENSIONS = 128;

    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return IbmWatsonxEmbeddingsServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String projectId,
        @Nullable String url,
        @Nullable String apiVersion,
        @Nullable Integer rateLimit
    ) {
        return buildServiceSettingsMap(modelId, projectId, url, apiVersion, null, null, null, rateLimit);
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings createServiceSettings(
        URI uri,
        String apiVersion,
        String modelId,
        String projectId,
        RateLimitSettings rateLimitSettings
    ) {
        return new IbmWatsonxEmbeddingsServiceSettings(modelId, projectId, uri, apiVersion, null, null, null, rateLimitSettings);
    }

    @Override
    protected List<String> additionalImmutableFields() {
        return List.of(ServiceFields.DIMENSIONS, ServiceFields.SIMILARITY);
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = IbmWatsonxEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_PROJECT_ID,
                TEST_URI.toString(),
                TEST_API_VERSION,
                TEST_MAX_INPUT_TOKENS,
                TEST_DIMENSIONS,
                TEST_SIMILARITY.toString(),
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

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
                    TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_ZeroDimensions_ThrowsException() {
        assertFromMap_NonPositiveField_ThrowsException(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, TEST_URI.toString(), TEST_API_VERSION, null, 0, null, null),
            ServiceFields.DIMENSIONS,
            0
        );
    }

    public void testFromMap_NegativeDimensions_ThrowsException() {
        var negativeDimensions = randomNegativeInt();
        assertFromMap_NonPositiveField_ThrowsException(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_PROJECT_ID,
                TEST_URI.toString(),
                TEST_API_VERSION,
                null,
                negativeDimensions,
                null,
                null
            ),
            ServiceFields.DIMENSIONS,
            negativeDimensions
        );
    }

    public void testFromMap_ZeroMaxInputTokens_ThrowsException() {
        assertFromMap_NonPositiveField_ThrowsException(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, TEST_URI.toString(), TEST_API_VERSION, 0, null, null, null),
            ServiceFields.MAX_INPUT_TOKENS,
            0
        );
    }

    public void testFromMap_NegativeMaxInputTokens_ThrowsException() {
        var negativeMaxInputTokens = randomNegativeInt();
        assertFromMap_NonPositiveField_ThrowsException(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_PROJECT_ID,
                TEST_URI.toString(),
                TEST_API_VERSION,
                negativeMaxInputTokens,
                null,
                null,
                null
            ),
            ServiceFields.MAX_INPUT_TOKENS,
            negativeMaxInputTokens
        );
    }

    public void testFromMap_NoDimensions_Success() {
        var serviceSettings = IbmWatsonxEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_PROJECT_ID,
                TEST_URI.toString(),
                TEST_API_VERSION,
                TEST_MAX_INPUT_TOKENS,
                null,
                TEST_SIMILARITY.toString(),
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    TEST_URI,
                    TEST_API_VERSION,
                    TEST_MAX_INPUT_TOKENS,
                    null,
                    TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoSimilarity_Success() {
        var serviceSettings = IbmWatsonxEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_PROJECT_ID,
                TEST_URI.toString(),
                TEST_API_VERSION,
                TEST_MAX_INPUT_TOKENS,
                TEST_DIMENSIONS,
                null,
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

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
                    null,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoMaxInputTokens_Success() {
        var serviceSettings = IbmWatsonxEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_PROJECT_ID,
                TEST_URI.toString(),
                TEST_API_VERSION,
                null,
                TEST_DIMENSIONS,
                TEST_SIMILARITY.toString(),
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    TEST_URI,
                    TEST_API_VERSION,
                    null,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    private void assertFromMap_NonPositiveField_ThrowsException(Map<String, Object> map, String fieldName, int value) {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> IbmWatsonxEmbeddingsServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );
        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, fieldName))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    value,
                    fieldName
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        var invalidSimilarity = "by_size";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> IbmWatsonxEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    TEST_URI.toString(),
                    TEST_API_VERSION,
                    null,
                    null,
                    invalidSimilarity,
                    null
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.SIMILARITY))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(Strings.format("Invalid value [%s]; expected one of [cosine, dot_product, l2_norm]", invalidSimilarity))
        );
    }

    public void testUpdateServiceSettings_MutableFields_AreUpdated() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var originalServiceSettings = createInitialSettings(INITIAL_TEST_MAX_INPUT_TOKENS, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        assertThat(
            originalServiceSettings.updateServiceSettings(settingsMap),
            is(createInitialSettings(TEST_MAX_INPUT_TOKENS, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_RateLimitOmitted_KeepsExistingRateLimit() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        var originalServiceSettings = createInitialSettings(INITIAL_TEST_MAX_INPUT_TOKENS, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        assertThat(
            originalServiceSettings.updateServiceSettings(settingsMap),
            is(createInitialSettings(TEST_MAX_INPUT_TOKENS, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_ExplicitNullMaxInputTokens_ClearsField() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(ServiceFields.MAX_INPUT_TOKENS, null);
        var originalServiceSettings = createInitialSettings(INITIAL_TEST_MAX_INPUT_TOKENS, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        assertThat(
            originalServiceSettings.updateServiceSettings(settingsMap),
            is(createInitialSettings(null, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_ZeroMaxInputTokens_ThrowsException() {
        assertUpdateServiceSettings_NonPositiveMaxInputTokens_ThrowsException(0);
    }

    public void testUpdateServiceSettings_NegativeMaxInputTokens_ThrowsException() {
        assertUpdateServiceSettings_NonPositiveMaxInputTokens_ThrowsException(randomNegativeInt());
    }

    private void assertUpdateServiceSettings_NonPositiveMaxInputTokens_ThrowsException(int nonPositiveMaxInputTokens) {
        var serviceSettings = createInitialSettings(INITIAL_TEST_MAX_INPUT_TOKENS, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        var e = expectThrows(
            XContentParseException.class,
            () -> serviceSettings.updateServiceSettings(new HashMap<>(Map.of(ServiceFields.MAX_INPUT_TOKENS, nonPositiveMaxInputTokens)))
        );
        assertThat(
            e.getMessage(),
            endsWith(
                Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.MAX_INPUT_TOKENS)
            )
        );
        assertThat(
            e.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    nonPositiveMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
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
            TEST_SIMILARITY,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            equalToIgnoringWhitespaceInJsonString(
                Strings.format(
                    """
                        {
                            "url": "%s",
                            "api_version": "%s",
                            "model_id": "%s",
                            "project_id": "%s",
                            "rate_limit": {
                                "requests_per_minute": %d
                            },
                            "max_input_tokens": %d,
                            "dimensions": %d,
                            "similarity": "%s"
                        }
                        """,
                    TEST_URI.toString(),
                    TEST_API_VERSION,
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    TEST_RATE_LIMIT,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY.toString()
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
        var uri = instance.uri();
        var apiVersion = instance.apiVersion();
        var maxInputTokens = instance.maxInputTokens();
        var dimensions = instance.dimensions();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(7)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(8));
            case 2 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(10) + ".example"));
            case 3 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(1, 256), null));
            case 5 -> dimensions = randomValueOtherThan(dimensions, () -> randomFrom(randomIntBetween(1, 256), null));
            case 6 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomFrom(SimilarityMeasure.values()), null));
            case 7 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new IbmWatsonxEmbeddingsServiceSettings(
            modelId,
            projectId,
            uri,
            apiVersion,
            maxInputTokens,
            dimensions,
            similarity,
            rateLimitSettings
        );
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings mutateInstanceForVersion(
        IbmWatsonxEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return IbmWatsonxEmbeddingsServiceSettings.createParser(true).apply(parser, ConfigurationParseContext.PERSISTENT).build();
    }

    private static IbmWatsonxEmbeddingsServiceSettings createInitialSettings(
        @Nullable Integer maxInputTokens,
        RateLimitSettings rateLimitSettings
    ) {
        return new IbmWatsonxEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            maxInputTokens,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY,
            rateLimitSettings
        );
    }

    private static IbmWatsonxEmbeddingsServiceSettings createRandom() {
        return new IbmWatsonxEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            createUri("https://" + randomAlphaOfLength(10) + ".example"),
            randomAlphaOfLength(8),
            randomFrom(randomIntBetween(1, 256), null),
            randomFrom(randomIntBetween(1, 256), null),
            randomFrom(randomFrom(SimilarityMeasure.values()), null),
            RateLimitSettingsTests.createRandom()
        );
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String projectId,
        @Nullable String url,
        @Nullable String apiVersion,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable String similarity,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (projectId != null) {
            map.put(IbmWatsonxServiceFields.PROJECT_ID, projectId);
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (apiVersion != null) {
            map.put(IbmWatsonxServiceFields.API_VERSION, apiVersion);
        }
        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            map.put(ServiceFields.SIMILARITY, similarity);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
