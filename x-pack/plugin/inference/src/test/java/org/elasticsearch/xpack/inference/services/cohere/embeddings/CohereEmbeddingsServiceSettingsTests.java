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
import org.elasticsearch.xpack.inference.services.ServiceUtils;
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

/**
 * {@link CohereServiceSettings} accepts a model identifier under either:
 * <ul>
 *     <li>{@link ServiceFields#MODEL_ID} ({@code "model_id"}) — current field name</li>
 *     <li>{@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"}) — legacy / deprecated key, still accepted for
 *         persisted configs</li>
 * </ul>
 * When both appear, {@code model_id} is used. Several {@code testFromMap_*} methods below spell out which key(s) each scenario uses.
 * <p>
 * {@code fromMap} tests that use {@link ConfigurationParseContext#PERSISTENT} are named {@code testFromMap_Persistent_*}
 * (Azure AI Studio service-settings tests use {@code testFromMap_Request_*} where parsing runs in request context).
 */
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
    private static final CohereServiceSettings.CohereApiVersion INITIAL_TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V1;
    private static final CohereServiceSettings.CohereApiVersion TEST_COHERE_API_VERSION = CohereServiceSettings.CohereApiVersion.V2;
    private static final CohereEmbeddingType INITIAL_TEST_COHERE_EMBEDDING_TYPE = CohereEmbeddingType.BIT;
    private static final CohereEmbeddingType TEST_COHERE_EMBEDDING_TYPE = CohereEmbeddingType.BYTE;
    /** Value placed under the legacy {@code model} key when testing precedence against {@link ServiceFields#MODEL_ID}. */
    private static final String LEGACY_MODEL_KEY_VALUE = "old_model";
    // Mirrors CohereServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS (used by CohereEmbeddingsServiceSettings#fromMap).
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
                INITIAL_TEST_COHERE_API_VERSION
            ),
            INITIAL_TEST_COHERE_EMBEDDING_TYPE
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMapWithModelIdField(TEST_COHERE_EMBEDDING_TYPE.toString(), TEST_COHERE_API_VERSION.toString())
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
                        INITIAL_TEST_COHERE_API_VERSION
                    ),
                    INITIAL_TEST_COHERE_EMBEDDING_TYPE
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
                INITIAL_TEST_COHERE_API_VERSION
            ),
            INITIAL_TEST_COHERE_EMBEDDING_TYPE
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
                        INITIAL_TEST_COHERE_API_VERSION
                    ),
                    CohereEmbeddingType.FLOAT
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#PERSISTENT}: full map using {@link ServiceFields#MODEL_ID} only (no legacy {@code model} key).
     */
    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMapWithModelIdField(TEST_COHERE_EMBEDDING_TYPE.toString(), TEST_COHERE_API_VERSION.toString()),
            ConfigurationParseContext.PERSISTENT
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
                        TEST_MODEL_ID,
                        new RateLimitSettings(TEST_RATE_LIMIT),
                        TEST_COHERE_API_VERSION
                    ),
                    TEST_COHERE_EMBEDDING_TYPE
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#PERSISTENT}: map contains only the legacy {@link CohereServiceSettings#OLD_MODEL_ID_FIELD}
     * ({@code "model"}) — no {@link ServiceFields#MODEL_ID}. The value is still normalized into {@link CohereServiceSettings#modelId()}.
     */
    public void testFromMap_Persistent_LegacyModelFieldOnly() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMapWithLegacyModelFieldOnly(DenseVectorFieldMapper.ElementType.BYTE.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        ServiceUtils.createUri(TEST_URL),
                        INITIAL_TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        null,
                        INITIAL_TEST_COHERE_API_VERSION
                    ),
                    CohereEmbeddingType.BYTE
                )
            )
        );
    }

    /**
     * {@link ConfigurationParseContext#REQUEST}: same as {@link #testFromMap_Persistent_LegacyModelFieldOnly}
     * (legacy {@code model} key only), but request parsing defaults the Cohere API version to
     * {@link CohereServiceSettings.CohereApiVersion#V2}.
     */
    public void testFromMap_Request_LegacyModelFieldOnly() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMapWithLegacyModelFieldOnly(CohereEmbeddingType.INT8.toString()),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        ServiceUtils.createUri(TEST_URL),
                        INITIAL_TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        null,
                        TEST_COHERE_API_VERSION
                    ),
                    CohereEmbeddingType.INT8
                )
            )
        );
    }

    /**
     * When both {@link ServiceFields#MODEL_ID} and the legacy {@code model} field are present, the {@code model_id} value wins.
     */
    public void testFromMap_WhenBothModelFieldsPresent_PrefersModelId() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMapWithBothModelFields(CohereEmbeddingType.BYTE.toString(), INITIAL_TEST_COHERE_API_VERSION.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        ServiceUtils.createUri(TEST_URL),
                        INITIAL_TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        null,
                        INITIAL_TEST_COHERE_API_VERSION
                    ),
                    CohereEmbeddingType.BYTE
                )
            )
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

    public void testFromMap_PersistentReadsInt8() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.EMBEDDING_TYPE, "int8")),
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
                TEST_COHERE_API_VERSION
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
                        TEST_COHERE_API_VERSION,
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

    /**
     * Map uses {@link ServiceFields#MODEL_ID} ({@code "model_id"}) when {@code modelId} is non-null; the legacy
     * {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"}) is omitted.
     */
    private static HashMap<String, Object> buildServiceSettingsMapWithModelIdField(
        String embeddingTypeString,
        @Nullable String apiVersion
    ) {
        return buildServiceSettingsMap(
            CohereEmbeddingsServiceSettingsTests.TEST_SIMILARITY_MEASURE,
            CohereEmbeddingsServiceSettingsTests.TEST_MODEL_ID,
            null,
            embeddingTypeString,
            apiVersion,
            CohereEmbeddingsServiceSettingsTests.TEST_RATE_LIMIT
        );
    }

    /**
     * Map uses only the legacy {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"});
     * {@link ServiceFields#MODEL_ID} is omitted.
     */
    private static HashMap<String, Object> buildServiceSettingsMapWithLegacyModelFieldOnly(String embeddingTypeString) {
        return buildServiceSettingsMap(
            CohereEmbeddingsServiceSettingsTests.INITIAL_TEST_SIMILARITY_MEASURE,
            null,
            CohereEmbeddingsServiceSettingsTests.TEST_MODEL_ID,
            embeddingTypeString,
            null,
            null
        );
    }

    /**
     * Map includes both {@link ServiceFields#MODEL_ID} and the legacy {@code model} key — production code must prefer {@code model_id}.
     */
    private static HashMap<String, Object> buildServiceSettingsMapWithBothModelFields(String embeddingTypeString, String apiVersion) {
        return buildServiceSettingsMap(
            CohereEmbeddingsServiceSettingsTests.INITIAL_TEST_SIMILARITY_MEASURE,
            CohereEmbeddingsServiceSettingsTests.TEST_MODEL_ID,
            CohereEmbeddingsServiceSettingsTests.LEGACY_MODEL_KEY_VALUE,
            embeddingTypeString,
            apiVersion,
            null
        );
    }

    /**
     * @param modelId if non-null, stored under {@link ServiceFields#MODEL_ID}
     * @param legacyModelValue if non-null, stored under {@link CohereServiceSettings#OLD_MODEL_ID_FIELD} ({@code "model"})
     */
    private static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable SimilarityMeasure similarity,
        @Nullable String modelId,
        @Nullable String legacyModelValue,
        @Nullable String embeddingTypeString,
        @Nullable String apiVersion,
        @Nullable Integer rateLimitRequestsPerMinute
    ) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.URL, CohereEmbeddingsServiceSettingsTests.TEST_URL);
        if (similarity != null) {
            result.put(ServiceFields.SIMILARITY, similarity.toString());
        }
        result.put(ServiceFields.DIMENSIONS, CohereEmbeddingsServiceSettingsTests.TEST_DIMENSIONS);
        result.put(ServiceFields.MAX_INPUT_TOKENS, CohereEmbeddingsServiceSettingsTests.TEST_MAX_INPUT_TOKENS);
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (legacyModelValue != null) {
            result.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, legacyModelValue);
        }
        if (embeddingTypeString != null) {
            result.put(ServiceFields.EMBEDDING_TYPE, embeddingTypeString);
        }
        if (apiVersion != null) {
            result.put(CohereServiceSettings.API_VERSION, apiVersion);
        }
        if (rateLimitRequestsPerMinute != null) {
            result.put(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimitRequestsPerMinute))
            );
        }
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
