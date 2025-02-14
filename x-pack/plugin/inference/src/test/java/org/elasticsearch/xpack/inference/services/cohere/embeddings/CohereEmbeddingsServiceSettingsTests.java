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
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<CohereEmbeddingsServiceSettings> {
    public static CohereEmbeddingsServiceSettings createRandom() {
        var commonSettings = CohereServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(CohereEmbeddingType.values());

        return new CohereEmbeddingsServiceSettings(commonSettings, embeddingType);
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CohereServiceSettings.OLD_MODEL_ID_FIELD,
                    model,
                    CohereEmbeddingsServiceSettings.EMBEDDING_TYPE,
                    DenseVectorFieldMapper.ElementType.BYTE.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        ServiceUtils.createUri(url),
                        SimilarityMeasure.DOT_PRODUCT,
                        dims,
                        maxInputTokens,
                        model,
                        null
                    ),
                    CohereEmbeddingType.BYTE
                )
            )
        );
    }

    public void testFromMap_WithModelId() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CohereServiceSettings.OLD_MODEL_ID_FIELD,
                    model,
                    CohereEmbeddingsServiceSettings.EMBEDDING_TYPE,
                    CohereEmbeddingType.INT8.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        ServiceUtils.createUri(url),
                        SimilarityMeasure.DOT_PRODUCT,
                        dims,
                        maxInputTokens,
                        model,
                        null
                    ),
                    CohereEmbeddingType.INT8
                )
            )
        );
    }

    public void testFromMap_PrefersModelId_OverModel() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CohereServiceSettings.OLD_MODEL_ID_FIELD,
                    "old_model",
                    CohereServiceSettings.MODEL_ID,
                    model,
                    CohereEmbeddingsServiceSettings.EMBEDDING_TYPE,
                    CohereEmbeddingType.BYTE.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(
                        ServiceUtils.createUri(url),
                        SimilarityMeasure.DOT_PRODUCT,
                        dims,
                        maxInputTokens,
                        model,
                        null
                    ),
                    CohereEmbeddingType.BYTE
                )
            )
        );
    }

    public void testFromMap_MissingEmbeddingType_DefaultsToFloat() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertThat(serviceSettings.getEmbeddingType(), is(CohereEmbeddingType.FLOAT));
    }

    public void testFromMap_EmptyEmbeddingType_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, "")),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    CohereEmbeddingsServiceSettings.EMBEDDING_TYPE
                )
            )
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_ForRequest() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, "abc")),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [abc] received. "
                        + "[embedding_type] must be one of [binary, bit, byte, float, int8];"
                )
            )
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_ForPersistent() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, "abc")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [abc] received. "
                        + "[embedding_type] must be one of [bit, byte, float];"
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenEmbeddingTypesAreNotValid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, List.of("abc"))),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            containsString("field [embedding_type] is not of the expected type. The value [[abc]] cannot be converted to a [String]")
        );
    }

    public void testFromMap_ConvertsElementTypeByte_ToCohereEmbeddingTypeByte() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, DenseVectorFieldMapper.ElementType.BYTE.toString())),
                ConfigurationParseContext.PERSISTENT
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings(), CohereEmbeddingType.BYTE))
        );
    }

    public void testFromMap_ConvertsElementTypeFloat_ToCohereEmbeddingTypeFloat() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, DenseVectorFieldMapper.ElementType.FLOAT.toString())),
                ConfigurationParseContext.PERSISTENT
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings(), CohereEmbeddingType.FLOAT))
        );
    }

    public void testFromMap_ConvertsInt8_ToCohereEmbeddingTypeInt8() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, CohereEmbeddingType.INT8.toString())),
                ConfigurationParseContext.REQUEST
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings(), CohereEmbeddingType.INT8))
        );
    }

    public void testFromMap_ConvertsBit_ToCohereEmbeddingTypeBit() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, CohereEmbeddingType.BIT.toString())),
                ConfigurationParseContext.REQUEST
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings(), CohereEmbeddingType.BIT))
        );
    }

    public void testFromMap_PreservesEmbeddingTypeFloat() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, CohereEmbeddingType.FLOAT.toString())),
                ConfigurationParseContext.REQUEST
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings(), CohereEmbeddingType.FLOAT))
        );
    }

    public void testFromMap_PersistentReadsInt8() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, "int8")),
                ConfigurationParseContext.PERSISTENT
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings(), CohereEmbeddingType.INT8))
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
            new CohereServiceSettings("url", SimilarityMeasure.COSINE, 5, 10, "model_id", new RateLimitSettings(3)),
            CohereEmbeddingType.INT8
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is("""
            {"url":"url","similarity":"cosine","dimensions":5,"max_input_tokens":10,"model_id":"model_id",""" + """
            "rate_limit":{"requests_per_minute":3},"embedding_type":"byte"}"""));
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
        return randomValueOtherThan(instance, CohereEmbeddingsServiceSettingsTests::createRandom);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model, @Nullable Enum<?> embeddingType) {
        var map = new HashMap<>(CohereServiceSettingsTests.getServiceSettingsMap(url, model));

        if (embeddingType != null) {
            map.put(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, embeddingType.toString());
        }

        return map;
    }
}
