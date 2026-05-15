/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereEmbeddingsServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    private static final CohereEmbeddingType TEST_EMBEDDING_TYPE = CohereEmbeddingType.BYTE;
    private static final CohereEmbeddingType INITIAL_TEST_EMBEDDING_TYPE = CohereEmbeddingType.BIT;

    private static final String TEST_LEGACY_MODEL_ID = "test-legacy-model-id";
    private static final RateLimitSettings DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereEmbeddingsServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = randomBoolean() ? null : randomFrom(SimilarityMeasure.values());
        Integer dimensions = randomBoolean() ? null : randomIntBetween(1, 2048);
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var modelId = randomAlphaOfLengthOrNull(15);
        var rateLimitSettings = RateLimitSettingsTests.createRandom();
        var apiVersion = randomFrom(CohereCommonServiceSettings.CohereApiVersion.values());
        var commonSettings = new CohereCommonServiceSettings(modelId, rateLimitSettings, apiVersion);
        var embeddingType = randomFrom(CohereEmbeddingType.values());

        return new CohereEmbeddingsServiceSettings(commonSettings, similarityMeasure, dimensions, maxInputTokens, embeddingType);
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
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
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_LEGACY_MODEL_ID,
                TEST_EMBEDDING_TYPE.toString(),
                CohereCommonServiceSettings.CohereApiVersion.V2
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

    public void testFromMap_Request_NullApiVersion_NewModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            null,
            null,
            ConfigurationParseContext.REQUEST,
            TEST_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_NullApiVersion_NewModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            null,
            null,
            ConfigurationParseContext.PERSISTENT,
            TEST_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V1
        );
    }

    public void testFromMap_V2_NewModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            null,
            CohereCommonServiceSettings.CohereApiVersion.V2,
            randomFrom(ConfigurationParseContext.values()),
            TEST_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Request_NullApiVersion_LegacyModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.REQUEST,
            TEST_LEGACY_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_NullApiVersion_LegacyModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.PERSISTENT,
            TEST_LEGACY_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V1
        );
    }

    public void testFromMap_V2_LegacyModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            TEST_LEGACY_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2,
            randomFrom(ConfigurationParseContext.values()),
            TEST_LEGACY_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Request_NullApiVersion_BothNewAndLegacyModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.REQUEST,
            TEST_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_NullApiVersion_BothNewAndLegacyModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.PERSISTENT,
            TEST_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V1
        );
    }

    public void testFromMap_Persistent_V2_BothNewAndLegacyModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            TEST_LEGACY_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2,
            ConfigurationParseContext.PERSISTENT,
            TEST_MODEL_ID,
            CohereCommonServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_V1_NoModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            null,
            CohereCommonServiceSettings.CohereApiVersion.V1,
            ConfigurationParseContext.PERSISTENT,
            null,
            CohereCommonServiceSettings.CohereApiVersion.V1
        );
    }

    private static void assertFromMap_CreatesSettingsCorrectly(
        String modelId,
        String legacyModelId,
        CohereCommonServiceSettings.CohereApiVersion apiVersion,
        ConfigurationParseContext context,
        String expectedModelId,
        CohereCommonServiceSettings.CohereApiVersion expectedApiVersion
    ) {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(modelId, legacyModelId, TEST_EMBEDDING_TYPE.toString(), apiVersion),
            context
        );

        assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereCommonServiceSettings(expectedModelId, new RateLimitSettings(TEST_RATE_LIMIT), expectedApiVersion),
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_EMBEDDING_TYPE
                )
            )
        );
    }

    public void testFromMap_Request_V2_NoModelIdFields_ThrowsMissingModelIdError() {
        assertFromMap_ThrowsMissingModelIdError(CohereCommonServiceSettings.CohereApiVersion.V2, ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_Persistent_V2_NoModelIdFields_ThrowsMissingModelIdError() {
        assertFromMap_ThrowsMissingModelIdError(CohereCommonServiceSettings.CohereApiVersion.V2, ConfigurationParseContext.PERSISTENT);
    }

    public void assertFromMap_ThrowsMissingModelIdError(
        CohereCommonServiceSettings.CohereApiVersion apiVersion,
        ConfigurationParseContext context
    ) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(null, null, TEST_EMBEDDING_TYPE.toString(), apiVersion),
                context
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Validation Failed: 1: The [service_settings.model_id] field is required for the Cohere V2 API.;")
        );
    }

    public void testFromMap_EmptyEmbeddingType_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "", ServiceFields.MODEL_ID, TEST_MODEL_ID)),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.EMBEDDING_TYPE
                )
            )
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_ForRequest() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "abc", ServiceFields.MODEL_ID, TEST_MODEL_ID)),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(thrownException.getMessage(), is(Strings.format("""
            Validation Failed: 1: [service_settings] Invalid value [abc] received. \
            [embedding_type] must be one of [binary, bit, byte, float, int8];""")));
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_ForPersistent() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "abc")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.getMessage(), is(Strings.format("""
            Validation Failed: 1: [service_settings] Invalid value [abc] received. \
            [embedding_type] must be one of [bit, byte, float];""")));
    }

    public void testFromMap_ReturnsFailure_WhenEmbeddingTypesAreNotValid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, List.of("abc"))),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            exception.getMessage(),
            containsString("field [embedding_type] is not of the expected type. The value [[abc]] cannot be converted to a [String]")
        );
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
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, DenseVectorFieldMapper.ElementType.BFLOAT16.toString())),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.getMessage(), is(Strings.format("""
            Validation Failed: 1: [service_settings] Invalid value [%s] received. \
            [embedding_type] must be one of [bit, byte, float];""", DenseVectorFieldMapper.ElementType.BFLOAT16.toString())));
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

    public void testFromCohereOrDenseVectorEnumValues() {
        var validation = new ValidationException();
        assertEquals(CohereEmbeddingType.BYTE, CohereEmbeddingsServiceSettings.fromCohereOrDenseVectorEnumValues("byte", validation));
        assertEquals(CohereEmbeddingType.INT8, CohereEmbeddingsServiceSettings.fromCohereOrDenseVectorEnumValues("int8", validation));
        assertEquals(CohereEmbeddingType.FLOAT, CohereEmbeddingsServiceSettings.fromCohereOrDenseVectorEnumValues("float", validation));
        assertEquals(CohereEmbeddingType.BINARY, CohereEmbeddingsServiceSettings.fromCohereOrDenseVectorEnumValues("binary", validation));
        assertEquals(CohereEmbeddingType.BIT, CohereEmbeddingsServiceSettings.fromCohereOrDenseVectorEnumValues("bit", validation));
        assertTrue(validation.validationErrors().isEmpty());
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

    public void testToXContentFragmentOfExposedFields_DoesNotWriteApiVersion() throws IOException {
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
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
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
                              "similarity": "%s",
                              "dimensions": %d,
                              "max_input_tokens": %d,
                              "embedding_type": "%s"
                            }
                            """,
                        TEST_MODEL_ID,
                        TEST_RATE_LIMIT,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        DenseVectorFieldMapper.ElementType.BYTE
                    )
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
        CohereCommonServiceSettings commonSettings = instance.getCommonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var embeddingType = instance.embeddingType();

        switch (randomInt(4)) {
            case 0 -> commonSettings = randomValueOtherThan(instance.getCommonSettings(), CohereCommonServiceSettingsTests::createRandom);
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()));
            case 2 -> dimensions = randomValueOtherThan(instance.dimensions(), () -> randomIntBetween(1, 4096));
            case 3 -> maxInputTokens = randomValueOtherThan(instance.maxInputTokens(), () -> randomIntBetween(128, 256));
            case 4 -> embeddingType = randomValueOtherThan(instance.embeddingType(), () -> randomFrom(CohereEmbeddingType.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens, embeddingType);
    }

    @Override
    protected CohereEmbeddingsServiceSettings mutateInstanceForVersion(CohereEmbeddingsServiceSettings instance, TransportVersion version) {
        var embeddingType = CohereEmbeddingType.translateToVersion(instance.embeddingType(), version);

        var commonSettings = instance.getCommonSettings();
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            commonSettings = new CohereCommonServiceSettings(
                instance.getCommonSettings().modelId(),
                instance.getCommonSettings().rateLimitSettings(),
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

    private static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String legacyModelId,
        @Nullable String embeddingType,
        @Nullable CohereCommonServiceSettings.CohereApiVersion apiVersion
    ) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.SIMILARITY, CohereEmbeddingsServiceSettingsTests.TEST_SIMILARITY_MEASURE.toString());
        result.put(ServiceFields.DIMENSIONS, TEST_DIMENSIONS);
        result.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (legacyModelId != null) {
            result.put(CohereCommonServiceSettings.OLD_MODEL_ID_FIELD, legacyModelId);
        }
        if (embeddingType != null) {
            result.put(ServiceFields.EMBEDDING_TYPE, embeddingType);
        }
        if (apiVersion != null) {
            result.put(CohereCommonServiceSettings.API_VERSION, apiVersion.toString());
        }
        result.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, CohereEmbeddingsServiceSettingsTests.TEST_RATE_LIMIT))
        );
        return result;
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
}
