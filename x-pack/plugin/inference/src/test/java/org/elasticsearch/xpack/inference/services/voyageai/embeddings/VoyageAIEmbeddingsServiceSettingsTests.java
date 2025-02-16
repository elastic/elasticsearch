/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class VoyageAIEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<VoyageAIEmbeddingsServiceSettings> {
    public static VoyageAIEmbeddingsServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
        dims = 1024;
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = VoyageAIServiceSettingsTests.createRandom();

        return new VoyageAIEmbeddingsServiceSettings(commonSettings, VoyageAIEmbeddingType.FLOAT, similarityMeasure, dims, maxInputTokens);
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
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
                    VoyageAIServiceSettings.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAIServiceSettings(ServiceUtils.createUri(url), model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens
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
        var serviceSettings = VoyageAIEmbeddingsServiceSettings.fromMap(
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
                    VoyageAIServiceSettings.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingsServiceSettings(
                    new VoyageAIServiceSettings(ServiceUtils.createUri(url), model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(VoyageAIServiceSettings.MODEL_ID, "model", ServiceFields.SIMILARITY, similarity)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. [similarity] "
                    + "must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAIEmbeddingsServiceSettings(
            new VoyageAIServiceSettings("url", "model", new RateLimitSettings(3)),
            VoyageAIEmbeddingType.FLOAT,
            SimilarityMeasure.COSINE,
            5,
            10
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(
            xContentResult,
            is(
                """
                    {"url":"url","model_id":"model","""
                    + """
                        "rate_limit":{"requests_per_minute":3},"similarity":"cosine","dimensions":5,"max_input_tokens":10,"embedding_type":"float"}"""
            )
        );
    }

    @Override
    protected Writeable.Reader<VoyageAIEmbeddingsServiceSettings> instanceReader() {
        return VoyageAIEmbeddingsServiceSettings::new;
    }

    @Override
    protected VoyageAIEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIEmbeddingsServiceSettings mutateInstance(VoyageAIEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, VoyageAIEmbeddingsServiceSettingsTests::createRandom);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, String model) {
        return new HashMap<>(VoyageAIServiceSettingsTests.getServiceSettingsMap(url, model));
    }
}
