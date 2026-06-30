/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.jinaai.AbstractJinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class JinaAITextEmbeddingServiceSettingsTests extends AbstractJinaAIServiceSettingsTests<JinaAITextEmbeddingServiceSettings> {

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

    public static JinaAITextEmbeddingServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = randomBoolean() ? null : randomSimilarityMeasure();
        Integer dimensions = randomBoolean() ? null : randomIntBetween(32, 256);
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = JinaAICommonServiceSettingsTests.createRandom();
        var embeddingType = randomBoolean() ? null : randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();

        return new JinaAITextEmbeddingServiceSettings(
            commonSettings,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
    }

    public static JinaAITextEmbeddingServiceSettings createRandomWithNoNullValues() {
        SimilarityMeasure similarityMeasure = randomSimilarityMeasure();
        Integer dimensions = randomIntBetween(32, 256);
        Integer maxInputTokens = randomIntBetween(128, 256);

        var commonSettings = JinaAICommonServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();

        return new JinaAITextEmbeddingServiceSettings(
            commonSettings,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
    }

    @Override
    protected JinaAITextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return JinaAITextEmbeddingServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        return buildServiceSettingsMap(modelId, null, null, null, null, null, rateLimit);
    }

    @Override
    protected JinaAITextEmbeddingServiceSettings createServiceSettings(String modelId, RateLimitSettings rateLimitSettings) {
        return new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(modelId, rateLimitSettings),
            null,
            null,
            null,
            null,
            false
        );
    }

    @Override
    protected List<String> additionalImmutableFields() {
        return List.of(ServiceFields.SIMILARITY, ServiceFields.DIMENSIONS, ServiceFields.EMBEDDING_TYPE);
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        testFromMap(randomIntBetween(1, 10_000), PERSISTENT);
    }

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        testFromMap(randomIntBetween(1, 10_000), REQUEST);
    }

    public void testFromMap_Request_NoDimensions_AllFields_CreatesSettingsCorrectly() {
        testFromMap(null, REQUEST);
    }

    private static void testFromMap(Integer dimensions, ConfigurationParseContext parseContext) {
        var similarity = randomSimilarityMeasure();
        var maxInputTokens = randomIntBetween(1, 10_000);
        var model = randomAlphanumericOfLength(8);
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var requestsPerMinute = randomIntBetween(1, 10_000);
        var settingsMap = buildServiceSettingsMap(model, similarity, dimensions, null, maxInputTokens, embeddingType, requestsPerMinute);

        boolean dimensionsSetByUser;
        if (parseContext == REQUEST) {
            dimensionsSetByUser = dimensions != null;
        } else {
            dimensionsSetByUser = randomBoolean();
            settingsMap.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }

        var serviceSettings = JinaAITextEmbeddingServiceSettings.fromMap(settingsMap, parseContext);

        assertThat(
            serviceSettings,
            is(
                new JinaAITextEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
                    similarity,
                    dimensions,
                    maxInputTokens,
                    embeddingType,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError() {
        var embeddingType = "invalid";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> JinaAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID, EMBEDDING_TYPE, embeddingType)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, EMBEDDING_TYPE))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(Strings.format("Invalid value [%s]; expected one of [float, bit, binary]", embeddingType))
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> JinaAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID, SIMILARITY, similarity)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, SIMILARITY))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(Strings.format("Invalid value [%s]; expected one of [cosine, dot_product, l2_norm]", similarity))
        );
    }

    public void testFromMap_nonPositiveDimensions_ThrowsError() {
        var dimensions = randomIntBetween(-5, 0);
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> JinaAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID, DIMENSIONS, dimensions)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, DIMENSIONS))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    dimensions,
                    DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_Request_RejectsMultimodalModelField() {
        var e = expectThrows(
            XContentParseException.class,
            () -> JinaAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(MODEL_ID, TEST_MODEL_ID, MULTIMODAL_MODEL, true)),
                REQUEST
            )
        );
        assertThat(
            e.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, MULTIMODAL_MODEL))
        );
    }

    public void testFromMap_Persistent_IgnoresMultimodalModelField() {
        var settingsMap = new HashMap<String, Object>(Map.of(MODEL_ID, TEST_MODEL_ID, MULTIMODAL_MODEL, true));
        var serviceSettings = JinaAITextEmbeddingServiceSettings.fromMap(settingsMap, PERSISTENT);

        assertThat(serviceSettings.isMultimodal(), is(false));
    }

    public void testUpdateServiceSettings_MutableFields_AreUpdated() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var originalServiceSettings = new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new JinaAITextEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
                    INITIAL_TEST_SIMILARITY,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_EMBEDDING_TYPE,
                    INITIAL_TEST_DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testUpdateServiceSettings_RateLimitOmitted_KeepsExistingRateLimit() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        var originalServiceSettings = new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER
        );

        assertThat(
            originalServiceSettings.updateServiceSettings(settingsMap),
            is(
                new JinaAITextEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
                    INITIAL_TEST_SIMILARITY,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_EMBEDDING_TYPE,
                    INITIAL_TEST_DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testUpdateServiceSettings_ExplicitNullMaxInputTokens_ClearsField() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(MAX_INPUT_TOKENS, null);
        var originalServiceSettings = new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER
        );

        assertThat(
            originalServiceSettings.updateServiceSettings(settingsMap),
            is(
                new JinaAITextEmbeddingServiceSettings(
                    new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
                    INITIAL_TEST_SIMILARITY,
                    INITIAL_TEST_DIMENSIONS,
                    null,
                    INITIAL_TEST_EMBEDDING_TYPE,
                    INITIAL_TEST_DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testUpdateServiceSettings_NonPositiveMaxInputTokens_ThrowsException() {
        var nonPositiveMaxInputTokens = randomIntBetween(-5, 0);
        var serviceSettings = new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)),
            INITIAL_TEST_SIMILARITY,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE,
            INITIAL_TEST_DIMENSIONS_SET_BY_USER
        );

        var e = expectThrows(
            XContentParseException.class,
            () -> serviceSettings.updateServiceSettings(new HashMap<>(Map.of(MAX_INPUT_TOKENS, nonPositiveMaxInputTokens)))
        );
        assertThat(
            e.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, MAX_INPUT_TOKENS))
        );
        assertThat(
            e.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    nonPositiveMaxInputTokens,
                    MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_EMBEDDING_TYPE,
            TEST_DIMENSIONS_SET_BY_USER
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
                            "dimensions_set_by_user": %b
                        }
                        """,
                    TEST_MODEL_ID,
                    TEST_RATE_LIMIT,
                    TEST_DIMENSIONS,
                    TEST_EMBEDDING_TYPE,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    TEST_DIMENSIONS_SET_BY_USER
                )
            )
        );
    }

    public void testToXContentFragmentOfExposedFields_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAITextEmbeddingServiceSettings(
            new JinaAICommonServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)),
            TEST_SIMILARITY,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_EMBEDDING_TYPE,
            TEST_DIMENSIONS_SET_BY_USER
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                },
                "dimensions": %d,
                "embedding_type": "%s",
                "max_input_tokens": %d,
                "similarity": "%s"
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT, TEST_DIMENSIONS, TEST_EMBEDDING_TYPE, TEST_MAX_INPUT_TOKENS, TEST_SIMILARITY)));
    }

    public void testUpdate() {
        var settings = createRandom();
        var similarity = randomSimilarityMeasure();
        var dimensions = randomIntBetween(32, 256);

        var newSettings = settings.update(similarity, dimensions);

        var expectedSettings = new JinaAITextEmbeddingServiceSettings(
            settings.getCommonSettings(),
            similarity,
            dimensions,
            settings.maxInputTokens(),
            settings.getEmbeddingType(),
            settings.dimensionsSetByUser()
        );

        assertThat(newSettings, is(expectedSettings));
    }

    @Override
    protected Writeable.Reader<JinaAITextEmbeddingServiceSettings> instanceReader() {
        return JinaAITextEmbeddingServiceSettings::new;
    }

    @Override
    protected JinaAITextEmbeddingServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAITextEmbeddingServiceSettings mutateInstance(JinaAITextEmbeddingServiceSettings instance) throws IOException {
        var commonSettings = instance.getCommonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var embeddingType = instance.getEmbeddingType();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        switch (randomInt(5)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, JinaAICommonServiceSettingsTests::createRandom);
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 2 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 4 -> embeddingType = randomValueOtherThan(embeddingType, () -> randomFrom(JinaAIEmbeddingType.values()));
            case 5 -> dimensionsSetByUser = dimensionsSetByUser == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAITextEmbeddingServiceSettings(
            commonSettings,
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
    }

    @Override
    protected JinaAITextEmbeddingServiceSettings mutateInstanceForVersion(
        JinaAITextEmbeddingServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED)) {
            return instance;
        }

        JinaAIEmbeddingType embeddingType;
        if (version.supports(JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)) {
            embeddingType = instance.getEmbeddingType();
        } else {
            // default to null embedding type if node is on a version before embedding type was introduced
            embeddingType = null;
        }

        return new JinaAITextEmbeddingServiceSettings(
            instance.getCommonSettings(),
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            embeddingType,
            false
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
        @Nullable Integer rateLimit
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
        return map;
    }
}
