/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIEmbeddingsServiceSettings> {
    public static JinaAIEmbeddingsServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
        dims = 1024;
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = JinaAIServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());

        return new JinaAIEmbeddingsServiceSettings(commonSettings, similarityMeasure, dims, maxInputTokens, embeddingType);
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
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
                    JinaAIServiceSettings.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(ServiceUtils.createUri(url), model, null),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    JinaAIEmbeddingType.FLOAT
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
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
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
                    JinaAIServiceSettings.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(ServiceUtils.createUri(url), model, null),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    JinaAIEmbeddingType.FLOAT
                )
            )
        );
    }

    public void testFromMap_WithEmbeddingType() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
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
                    JinaAIServiceSettings.MODEL_ID,
                    model,
                    JinaAIEmbeddingsServiceSettings.EMBEDDING_TYPE,
                    JinaAIEmbeddingType.BIT.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(ServiceUtils.createUri(url), model, null),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    JinaAIEmbeddingType.BIT
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(JinaAIServiceSettings.MODEL_ID, "model", ServiceFields.SIMILARITY, similarity)),
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

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new JinaAIEmbeddingsServiceSettings(
            new JinaAIServiceSettings("url", "model", new RateLimitSettings(3)),
            SimilarityMeasure.COSINE,
            5,
            10,
            JinaAIEmbeddingType.FLOAT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is("""
            {"url":"url","model_id":"model",""" + """
            "rate_limit":{"requests_per_minute":3},""" + """
            "similarity":"cosine","dimensions":5,"max_input_tokens":10,"embedding_type":"float"}"""));
    }

    @Override
    protected Writeable.Reader<JinaAIEmbeddingsServiceSettings> instanceReader() {
        return JinaAIEmbeddingsServiceSettings::new;
    }

    @Override
    protected JinaAIEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIEmbeddingsServiceSettings mutateInstance(JinaAIEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, JinaAIEmbeddingsServiceSettingsTests::createRandom);
    }

    @Override
    protected JinaAIEmbeddingsServiceSettings mutateInstanceForVersion(JinaAIEmbeddingsServiceSettings instance, TransportVersion version) {
        if (version.onOrAfter(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)
            || version.isPatchFrom(TransportVersions.JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_19)) {
            return instance;
        }

        // default to null embedding type if node is on a version before embedding type was introduced
        return new JinaAIEmbeddingsServiceSettings(
            instance.getCommonSettings(),
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            null
        );
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
        String model,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        var map = new HashMap<>(JinaAIServiceSettingsTests.getServiceSettingsMap(url, model));

        if (embeddingType != null) {
            map.put(JinaAIEmbeddingsServiceSettings.EMBEDDING_TYPE, embeddingType.toString());
        }

        return map;
    }
}
