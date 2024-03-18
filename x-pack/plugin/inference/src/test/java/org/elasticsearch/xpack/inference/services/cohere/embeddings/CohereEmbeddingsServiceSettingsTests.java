/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<CohereEmbeddingsServiceSettings> {
    public static CohereEmbeddingsServiceSettings createRandom() {
        var commonSettings = CohereServiceSettingsTests.createRandom();
        var embeddingType = randomBoolean() ? randomFrom(CohereEmbeddingType.values()) : null;

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
                    CohereEmbeddingType.INT8.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(ServiceUtils.createUri(url), SimilarityMeasure.DOT_PRODUCT, dims, maxInputTokens, model),
                    CohereEmbeddingType.INT8
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
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(ServiceUtils.createUri(url), SimilarityMeasure.DOT_PRODUCT, dims, maxInputTokens, model),
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
                    CohereEmbeddingType.INT8.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings(ServiceUtils.createUri(url), SimilarityMeasure.DOT_PRODUCT, dims, maxInputTokens, model),
                    CohereEmbeddingType.INT8
                )
            )
        );
    }

    public void testFromMap_MissingEmbeddingType_DoesNotThrowException() {
        var serviceSettings = CohereEmbeddingsServiceSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertNull(serviceSettings.getEmbeddingType());
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

    public void testFromMap_InvalidEmbeddingType_ThrowsError() {
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
                    "Validation Failed: 1: [service_settings] Invalid value [abc] received. [embedding_type] must be one of [float, int8];"
                )
            )
        );
    }

    public void testFromMap_InvalidEmbeddingType_ThrowsError_WhenByteFromPersistedConfig() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, CohereEmbeddingsServiceSettings.EMBEDDING_TYPE_BYTE)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [byte] received. [embedding_type] must be one of [float, int8];"
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenEmbeddingTypesAreNotValid() {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, List.of("abc"))),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is("field [embedding_type] is not of the expected type. The value [[abc]] cannot be converted to a [String]")
        );
    }

    public void testFromMap_ConvertsCohereEmbeddingType_FromByteToInt8() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, CohereEmbeddingsServiceSettings.EMBEDDING_TYPE_BYTE)),
                ConfigurationParseContext.REQUEST
            ),
            is(new CohereEmbeddingsServiceSettings(new CohereServiceSettings((URI) null, null, null, null, null), CohereEmbeddingType.INT8))
        );
    }

    public void testFromMap_PreservesEmbeddingTypeFloat() {
        assertThat(
            CohereEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, CohereEmbeddingType.FLOAT.toString())),
                ConfigurationParseContext.REQUEST
            ),
            is(
                new CohereEmbeddingsServiceSettings(
                    new CohereServiceSettings((URI) null, null, null, null, null),
                    CohereEmbeddingType.FLOAT
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
        return null;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(
        @Nullable String url,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType
    ) {
        var map = new HashMap<>(CohereServiceSettingsTests.getServiceSettingsMap(url, model));

        if (embeddingType != null) {
            map.put(CohereEmbeddingsServiceSettings.EMBEDDING_TYPE, embeddingType.toString());
        }

        return map;
    }
}
