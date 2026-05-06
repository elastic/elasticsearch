/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<CohereEmbeddingsServiceSettings> {
    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

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
        var commonSettings = CohereServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(CohereEmbeddingType.values());

        return new CohereEmbeddingsServiceSettings(commonSettings, embeddingType);
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new CohereEmbeddingsServiceSettings(
            new CohereServiceSettings(
                INITIAL_TEST_URL,
                INITIAL_TEST_SIMILARITY_MEASURE,
                INITIAL_TEST_DIMENSIONS,
                INITIAL_TEST_MAX_INPUT_TOKENS,
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereServiceSettings.CohereApiVersion.V1
            ),
            INITIAL_TEST_EMBEDDING_TYPE
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_LEGACY_MODEL_ID,
                TEST_EMBEDDING_TYPE.toString(),
                CohereServiceSettings.CohereApiVersion.V2
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        INITIAL_TEST_URL,
                        INITIAL_TEST_SIMILARITY_MEASURE,
                        INITIAL_TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        INITIAL_TEST_MODEL_ID,
                        new RateLimitSettings(TEST_RATE_LIMIT),
                        CohereServiceSettings.CohereApiVersion.V1
                    ),
                    INITIAL_TEST_EMBEDDING_TYPE
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new CohereEmbeddingsServiceSettings(
            new CohereServiceSettings(
                INITIAL_TEST_URL,
                INITIAL_TEST_SIMILARITY_MEASURE,
                INITIAL_TEST_DIMENSIONS,
                INITIAL_TEST_MAX_INPUT_TOKENS,
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereServiceSettings.CohereApiVersion.V1
            ),
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
                    new CohereServiceSettings(
                        (URI) null,
                        null,
                        null,
                        null,
                        null,
                        DEFAULT_COHERE_EMBEDDINGS_RATE_LIMIT_SETTINGS,
                        CohereServiceSettings.CohereApiVersion.V1
                    ),
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
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_NullApiVersion_NewModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            null,
            null,
            ConfigurationParseContext.PERSISTENT,
            TEST_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V1
        );
    }

    public void testFromMap_V2_NewModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            null,
            CohereServiceSettings.CohereApiVersion.V2,
            randomFrom(ConfigurationParseContext.values()),
            TEST_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Request_NullApiVersion_LegacyModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.REQUEST,
            TEST_LEGACY_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_NullApiVersion_LegacyModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.PERSISTENT,
            TEST_LEGACY_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V1
        );
    }

    public void testFromMap_V2_LegacyModelIdField_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            TEST_LEGACY_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2,
            randomFrom(ConfigurationParseContext.values()),
            TEST_LEGACY_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Request_NullApiVersion_BothNewAndLegacyModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.REQUEST,
            TEST_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_NullApiVersion_BothNewAndLegacyModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            TEST_LEGACY_MODEL_ID,
            null,
            ConfigurationParseContext.PERSISTENT,
            TEST_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V1
        );
    }

    public void testFromMap_Persistent_V2_BothNewAndLegacyModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            TEST_MODEL_ID,
            TEST_LEGACY_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2,
            ConfigurationParseContext.PERSISTENT,
            TEST_MODEL_ID,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testFromMap_Persistent_V1_NoModelIdFields_CreatesSettingsCorrectly() {
        assertFromMap_CreatesSettingsCorrectly(
            null,
            null,
            CohereServiceSettings.CohereApiVersion.V1,
            ConfigurationParseContext.PERSISTENT,
            null,
            CohereServiceSettings.CohereApiVersion.V1
        );
    }

    private static void assertFromMap_CreatesSettingsCorrectly(
        String modelId,
        String legacyModelId,
        CohereServiceSettings.CohereApiVersion apiVersion,
        ConfigurationParseContext context,
        String expectedModelId,
        CohereServiceSettings.CohereApiVersion expectedApiVersion
    ) {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(modelId, legacyModelId, TEST_EMBEDDING_TYPE.toString(), apiVersion),
            context
        );

        assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        TEST_URL,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        expectedModelId,
                        new RateLimitSettings(TEST_RATE_LIMIT),
                        expectedApiVersion
                    ),
                    TEST_EMBEDDING_TYPE
                )
            )
        );
    }

    public void testFromMap_Request_V2_NoModelIdFields_ThrowsMissingModelIdError() {
        assertFromMap_ThrowsMissingModelIdError(CohereServiceSettings.CohereApiVersion.V2, ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_Persistent_V2_NoModelIdFields_ThrowsMissingModelIdError() {
        assertFromMap_ThrowsMissingModelIdError(CohereServiceSettings.CohereApiVersion.V2, ConfigurationParseContext.PERSISTENT);
    }

    public void assertFromMap_ThrowsMissingModelIdError(
        CohereServiceSettings.CohereApiVersion apiVersion,
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
                    new CohereServiceSettings(CohereServiceSettings.CohereApiVersion.V1),
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
                    new CohereServiceSettings(CohereServiceSettings.CohereApiVersion.V1),
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
                    new CohereServiceSettings(CohereServiceSettings.CohereApiVersion.V1),
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
                    new CohereServiceSettings(
                        (String) null,
                        null,
                        null,
                        null,
                        TEST_MODEL_ID,
                        null,
                        CohereServiceSettings.CohereApiVersion.V2
                    ),
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
                    new CohereServiceSettings(
                        (String) null,
                        null,
                        null,
                        null,
                        TEST_MODEL_ID,
                        null,
                        CohereServiceSettings.CohereApiVersion.V2
                    ),
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
                    new CohereServiceSettings(
                        (String) null,
                        null,
                        null,
                        null,
                        TEST_MODEL_ID,
                        null,
                        CohereServiceSettings.CohereApiVersion.V2
                    ),
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
            new CohereServiceSettings(
                TEST_URL,
                TEST_SIMILARITY_MEASURE,
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                CohereServiceSettings.CohereApiVersion.V2
            ),
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
                              "url": "%s",
                              "similarity": "%s",
                              "dimensions": %d,
                              "max_input_tokens": %d,
                              "model_id": "%s",
                              "rate_limit": {
                                "requests_per_minute": %d
                              },
                              "api_version": "%s",
                              "embedding_type": "%s"
                            }
                            """,
                        TEST_URL,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        TEST_RATE_LIMIT,
                        CohereServiceSettings.CohereApiVersion.V2,
                        DenseVectorFieldMapper.ElementType.BYTE
                    )
                )
            )
        );
    }

    public void testToXContentFragmentOfExposedFields_DoesNotWriteApiVersion() throws IOException {
        var serviceSettings = new CohereEmbeddingsServiceSettings(
            new CohereServiceSettings(
                TEST_URL,
                TEST_SIMILARITY_MEASURE,
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                CohereServiceSettings.CohereApiVersion.V2
            ),
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
                              "url": "%s",
                              "similarity": "%s",
                              "dimensions": %d,
                              "max_input_tokens": %d,
                              "model_id": "%s",
                              "rate_limit": {
                                "requests_per_minute": %d
                              },
                              "embedding_type": "%s"
                            }
                            """,
                        TEST_URL,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        TEST_RATE_LIMIT,
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
        if (randomBoolean()) {
            CohereServiceSettings commonSettings = randomValueOtherThan(
                instance.getCommonSettings(),
                CohereServiceSettingsTests::createRandom
            );
            return new CohereEmbeddingsServiceSettings(commonSettings, instance.getEmbeddingType());
        } else {
            CohereEmbeddingType embeddingType = randomValueOtherThan(
                instance.getEmbeddingType(),
                () -> randomFrom(CohereEmbeddingType.values())
            );
            return new CohereEmbeddingsServiceSettings(instance.getCommonSettings(), embeddingType);
        }
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
        @Nullable CohereServiceSettings.CohereApiVersion apiVersion
    ) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.URL, TEST_URL);
        result.put(ServiceFields.SIMILARITY, CohereEmbeddingsServiceSettingsTests.TEST_SIMILARITY_MEASURE.toString());
        result.put(ServiceFields.DIMENSIONS, TEST_DIMENSIONS);
        result.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (legacyModelId != null) {
            result.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, legacyModelId);
        }
        if (embeddingType != null) {
            result.put(ServiceFields.EMBEDDING_TYPE, embeddingType);
        }
        if (apiVersion != null) {
            result.put(CohereServiceSettings.API_VERSION, apiVersion.toString());
        }
        result.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, CohereEmbeddingsServiceSettingsTests.TEST_RATE_LIMIT))
        );
        return result;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model, @Nullable Enum<?> embeddingType) {
        var map = new HashMap<>(CohereServiceSettingsTests.getServiceSettingsMap(url, model));

        if (embeddingType != null) {
            map.put(ServiceFields.EMBEDDING_TYPE, embeddingType.toString());
        }

        return map;
    }
}
