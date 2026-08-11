/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.AbstractCohereServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsServiceSettingsTests extends AbstractCohereServiceSettingsTests<CohereEmbeddingsServiceSettings> {

    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    private static final CohereEmbeddingType INITIAL_TEST_EMBEDDING_TYPE = CohereEmbeddingType.BIT;

    private static final RateLimitSettings DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereEmbeddingsServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = randomBoolean() ? null : randomFrom(SimilarityMeasure.values());
        Integer dimensions = randomBoolean() ? null : randomIntBetween(1, 2048);
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var apiVersion = randomFrom(CohereCommonServiceSettings.CohereApiVersion.values());
        var modelId = apiVersion == CohereCommonServiceSettings.CohereApiVersion.V2
            ? randomAlphaOfLength(15)
            : randomAlphaOfLengthOrNull(15);
        var rateLimitSettings = RateLimitSettingsTests.createRandom();
        var commonSettings = new CohereCommonServiceSettings(modelId, rateLimitSettings, apiVersion);
        var embeddingType = randomFrom(CohereEmbeddingType.values());

        return new CohereEmbeddingsServiceSettings(commonSettings, similarityMeasure, dimensions, maxInputTokens, embeddingType);
    }

    @Override
    protected CohereEmbeddingsServiceSettings createGivenCommonSettings(
        Map<String, Object> commonSettings,
        ConfigurationParseContext context
    ) {
        Map<String, Object> serviceSettings = new HashMap<>(commonSettings);
        CohereEmbeddingsServiceSettings randomInstance = createRandom();
        if (randomInstance.similarity() != null) {
            serviceSettings.put(ServiceFields.SIMILARITY, randomInstance.similarity());
        }
        if (randomInstance.dimensions() != null) {
            serviceSettings.put(ServiceFields.DIMENSIONS, randomInstance.dimensions());
        }
        if (randomInstance.maxInputTokens() != null) {
            serviceSettings.put(ServiceFields.MAX_INPUT_TOKENS, randomInstance.maxInputTokens());
        }
        serviceSettings.put(ServiceFields.EMBEDDING_TYPE, randomInstance.embeddingType());
        return CohereEmbeddingsServiceSettings.fromMap(serviceSettings, context);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(CohereEmbeddingsServiceSettings instance, XContentBuilder builder)
        throws IOException {
        return instance.toXContentFragmentOfExposedFields(builder, null);
    }

    @Override
    protected Set<String> getImmutableFields() {
        Set<String> immutableFields = new HashSet<>(super.getImmutableFields());
        immutableFields.add(ServiceFields.SIMILARITY);
        immutableFields.add(ServiceFields.DIMENSIONS);
        immutableFields.add(ServiceFields.EMBEDDING_TYPE);
        return immutableFields;
    }

    public void testUpdateServiceSettings_AllUpdatableFields() {
        var originalServiceSettings = new CohereEmbeddingsServiceSettings(
            new CohereCommonServiceSettings(
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
            ),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            Map.of(
                RateLimitSettings.FIELD_NAME,
                Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT),
                ServiceFields.MAX_INPUT_TOKENS,
                TEST_MAX_INPUT_TOKENS
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        INITIAL_TEST_MODEL_ID,
                        new RateLimitSettings(TEST_RATE_LIMIT),
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    ),
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_EMBEDDING_TYPE
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new CohereEmbeddingsServiceSettings(
            new CohereCommonServiceSettings(
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
            ),
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_EMBEDDING_TYPE
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Persistent_EmptyMap_CreatesSettingsCorrectly() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        null,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.FLOAT
                )
            )
        );
    }

    public void testFromMap_EmptyEmbeddingType_ThrowsError() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "", ServiceFields.MODEL_ID, TEST_MODEL_ID)),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.getMessage(), containsString("failed to parse field [embedding_type]"));
        assertThat(thrownException.getCause().getMessage(), containsString("Invalid value []; expected one of [byte, float, bit]"));
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_ForRequest() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "abc", ServiceFields.MODEL_ID, TEST_MODEL_ID)),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.getMessage(), containsString("failed to parse field [embedding_type]"));
        assertThat(thrownException.getCause().getMessage(), containsString("Invalid value [abc]; expected one of [byte, float, bit]"));
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_ForPersistent() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "abc")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.getMessage(), containsString("failed to parse field [embedding_type]"));
        assertThat(thrownException.getCause().getMessage(), containsString("Invalid value [abc]; expected one of [byte, float, bit]"));
    }

    public void testFromMap_ReturnsFailure_WhenEmbeddingTypesAreNotValid() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, List.of("abc"))),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(exception.getMessage(), containsString("embedding_type doesn't support values of type: START_ARRAY"));
    }

    public void testFromMap_ConvertsElementTypeByte_ToCohereEmbeddingTypeByte() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, DenseVectorFieldMapper.ElementType.BYTE.toString())),
                ConfigurationParseContext.PERSISTENT
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        null,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.BYTE
                )
            )
        );
    }

    public void testFromMap_ConvertsElementTypeFloat_ToCohereEmbeddingTypeFloat() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, DenseVectorFieldMapper.ElementType.FLOAT.toString())),
                ConfigurationParseContext.PERSISTENT
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        null,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.FLOAT
                )
            )
        );
    }

    public void testFromMap_ConvertsElementTypeBfloat16_ThrowsError_ForPersistent() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, DenseVectorFieldMapper.ElementType.BFLOAT16.toString())),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.getMessage(), containsString("failed to parse field [embedding_type]"));
        assertThat(thrownException.getCause().getMessage(), containsString("Invalid value [bfloat16]; expected one of [byte, float, bit]"));
    }

    public void testFromMap_ConvertsInt8_ToCohereEmbeddingTypeInt8() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, CohereEmbeddingType.INT8.toString())),
                ConfigurationParseContext.PERSISTENT
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        null,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.INT8
                )
            )
        );
    }

    public void testFromMap_ConvertsBit_ToCohereEmbeddingTypeBit() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(ServiceFields.EMBEDDING_TYPE, CohereEmbeddingType.BIT.toString(), ServiceFields.MODEL_ID, TEST_MODEL_ID)
                ),
                ConfigurationParseContext.REQUEST
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        TEST_MODEL_ID,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V2
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.BIT
                )
            )
        );
    }

    public void testFromMap_ConvertsBinary_ToCohereEmbeddingTypeBinary() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "binary", ServiceFields.MODEL_ID, TEST_MODEL_ID)),
                ConfigurationParseContext.REQUEST
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        TEST_MODEL_ID,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V2
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.BINARY
                )
            )
        );
    }

    public void testFromMap_PreservesEmbeddingTypeFloat() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(ServiceFields.EMBEDDING_TYPE, CohereEmbeddingType.FLOAT.toString(), ServiceFields.MODEL_ID, TEST_MODEL_ID)
                ),
                ConfigurationParseContext.REQUEST
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(
                        TEST_MODEL_ID,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V2
                    ),
                    null,
                    null,
                    null,
                    CohereEmbeddingType.FLOAT
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereEmbeddingsServiceSettings(
            new CohereCommonServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V2
            ),
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            CohereEmbeddingType.INT8
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {
                              "model_id": "%s",
                              "rate_limit": {
                                "requests_per_minute": %d
                              },
                              "api_version": "%s",
                              "similarity": "%s",
                              "dimensions": %d,
                              "max_input_tokens": %d,
                              "embedding_type": "%s"
                            }
                            """,
                        TEST_MODEL_ID,
                        TEST_RATE_LIMIT,
                        CohereCommonServiceSettings.CohereApiVersion.V2,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        DenseVectorFieldMapper.ElementType.BYTE
                    )
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        String invalidSimilarity = "abc";
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.SIMILARITY, invalidSimilarity, ServiceFields.MODEL_ID, TEST_MODEL_ID)),
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

    public void testFromMap_NegativeDimensions_ThrowsException() {
        var negativeDimensions = randomNegativeInt();
        assertFromMap_PositiveFieldSetToNonPositiveValue_ThrowsException(ServiceFields.DIMENSIONS, negativeDimensions);
    }

    public void testFromMap_ZeroDimensions_ThrowsException() {
        var zeroDimensions = 0;
        assertFromMap_PositiveFieldSetToNonPositiveValue_ThrowsException(ServiceFields.DIMENSIONS, zeroDimensions);
    }

    public void testFromMap_NegativeMaxInputTokens_ThrowsException() {
        var negativeMaxTokens = randomNegativeInt();
        assertFromMap_PositiveFieldSetToNonPositiveValue_ThrowsException(ServiceFields.MAX_INPUT_TOKENS, negativeMaxTokens);
    }

    public void testFromMap_ZeroMaxInputTokens_ThrowsException() {
        var zeroMaxTokens = 0;
        assertFromMap_PositiveFieldSetToNonPositiveValue_ThrowsException(ServiceFields.MAX_INPUT_TOKENS, zeroMaxTokens);
    }

    private static void assertFromMap_PositiveFieldSetToNonPositiveValue_ThrowsException(String fieldName, int value) {
        var context = randomFrom(ConfigurationParseContext.values());
        var e = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(Map.of(fieldName, value), context)
        );
        assertThat(
            e.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, fieldName))
        );
        assertThat(
            e.getCause().getMessage(),
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

    public void testUpdate_NegativeMaxInputTokens_ThrowsException() {
        assertUpdate_NonPositiveMaxInputTokens_ThrowsException(randomNegativeInt());
    }

    public void testUpdate_ZeroMaxInputTokens_ThrowsException() {
        assertUpdate_NonPositiveMaxInputTokens_ThrowsException(0);
    }

    private void assertUpdate_NonPositiveMaxInputTokens_ThrowsException(int maxInputTokens) {
        CohereEmbeddingsServiceSettings instance = createTestInstance();

        var e = expectThrows(
            XContentParseException.class,
            () -> instance.updateServiceSettings(Map.of(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens))
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
                    maxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<CohereEmbeddingsServiceSettings> instanceReader() {
        return CohereEmbeddingsServiceSettings::new;
    }

    @Override
    protected CohereEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereEmbeddingsServiceSettings mutateInstance(CohereEmbeddingsServiceSettings instance) throws IOException {
        CohereCommonServiceSettings commonSettings = instance.commonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var embeddingType = instance.embeddingType();

        switch (randomInt(4)) {
            case 0 -> commonSettings = randomValueOtherThan(instance.commonSettings(), CohereCommonServiceSettingsTests::createRandom);
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()));
            case 2 -> dimensions = randomValueOtherThan(instance.dimensions(), () -> randomIntBetween(1, 4096));
            case 3 -> maxInputTokens = randomValueOtherThan(instance.maxInputTokens(), () -> randomIntBetween(128, 256));
            case 4 -> embeddingType = randomValueOtherThan(
                instance.embeddingType(),
                () -> randomFrom(CohereEmbeddingType.values()).normalize()
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens, embeddingType);
    }

    @Override
    protected CohereEmbeddingsServiceSettings mutateInstanceForVersion(CohereEmbeddingsServiceSettings instance, TransportVersion version) {
        var embeddingType = CohereEmbeddingType.translateToVersion(instance.embeddingType(), version);

        var commonSettings = instance.commonSettings();
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            commonSettings = new CohereCommonServiceSettings(
                instance.commonSettings().modelId(),
                instance.commonSettings().rateLimitSettings(),
                CohereCommonServiceSettings.CohereApiVersion.V1
            );
        }
        return new CohereEmbeddingsServiceSettings(
            commonSettings,
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            embeddingType
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String model, @Nullable Enum<?> embeddingType) {
        var map = new HashMap<String, Object>();

        if (model != null) {
            map.put(CohereCommonServiceSettings.OLD_MODEL_ID_FIELD, model);
        }

        if (embeddingType != null) {
            map.put(ServiceFields.EMBEDDING_TYPE, embeddingType.toString());
        }

        return map;
    }

    @Override
    protected CohereEmbeddingsServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return CohereEmbeddingsServiceSettings.createParser(ignoreUnknownFields, PARSE_CONTEXT).apply(parser, PARSE_CONTEXT).build();
    }
}
