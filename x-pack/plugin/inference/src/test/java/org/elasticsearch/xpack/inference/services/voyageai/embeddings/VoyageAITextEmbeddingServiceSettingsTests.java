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
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.hamcrest.Matchers.is;

public class VoyageAITextEmbeddingServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAITextEmbeddingServiceSettings> {
    public static VoyageAITextEmbeddingServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
        Integer dims = 1024;
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        Boolean dimensionSetByUser = randomBoolean();

        var commonSettings = VoyageAIServiceSettingsTests.createRandom();

        return new VoyageAITextEmbeddingServiceSettings(
            commonSettings,
            VoyageAIEmbeddingType.FLOAT,
            similarityMeasure,
            dims,
            maxInputTokens,
            dimensionSetByUser
        );
    }

    public void testFromMap() {
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = VoyageAITextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAITextEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    false
                )
            )
        );
    }

    public void testFromMap_WithModelId() {
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = VoyageAITextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(ServiceFields.SIMILARITY, similarity, ServiceFields.MAX_INPUT_TOKENS, maxInputTokens, ServiceFields.MODEL_ID, model)
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAITextEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    null,
                    maxInputTokens,
                    false
                )
            )
        );
    }

    public void testFromMap_WithModelId_WithDimensions() {
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = VoyageAITextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAITextEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    true
                )
            )
        );
    }

    public void testFromMap_DimensionsSetByUserIsFalseInRequestContext() {
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = VoyageAITextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    DIMENSIONS_SET_BY_USER,
                    true,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAITextEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    null,
                    maxInputTokens,
                    false
                )
            )
        );
    }

    public void testFromMap_DimensionsSetByUserIsSetInPersistentContext() {
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var maxInputTokens = 512;
        var model = "model";
        var dimensionsSetByUser = randomBoolean();
        var serviceSettings = VoyageAITextEmbeddingServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    DIMENSIONS_SET_BY_USER,
                    dimensionsSetByUser,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new VoyageAITextEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    SimilarityMeasure.DOT_PRODUCT,
                    null,
                    maxInputTokens,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", ServiceFields.SIMILARITY, similarity)),
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

    public void testFromMap_InvalidEmbeddingType_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", ServiceFields.EMBEDDING_TYPE, "invalid_type")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(thrownException.getMessage(), org.hamcrest.Matchers.containsString("[embedding_type]"));
    }

    public void testFromMap_NegativeDimensions_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", ServiceFields.DIMENSIONS, -1)),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(thrownException.getMessage(), org.hamcrest.Matchers.containsString("[dimensions]"));
    }

    public void testFromMap_NegativeMaxInputTokens_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> VoyageAITextEmbeddingServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", ServiceFields.MAX_INPUT_TOKENS, -100)),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(thrownException.getMessage(), org.hamcrest.Matchers.containsString("[max_input_tokens]"));
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAITextEmbeddingServiceSettings(
            new VoyageAIServiceSettings("model", new RateLimitSettings(3)),
            VoyageAIEmbeddingType.FLOAT,
            SimilarityMeasure.COSINE,
            5,
            10,
            false
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(
            xContentResult,
            is(
                """
                    {"model_id":"model","""
                    + """
                        "rate_limit":{"requests_per_minute":3},"similarity":"cosine","dimensions":5,"max_input_tokens":10,"""
                    + """
                        "embedding_type":"float","dimensions_set_by_user":false}"""
            )
        );
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testToXContent_WritesAllValues_DimensionSetByUser() throws IOException {
        var serviceSettings = new VoyageAITextEmbeddingServiceSettings(
            new VoyageAIServiceSettings("model", new RateLimitSettings(3)),
            VoyageAIEmbeddingType.FLOAT,
            SimilarityMeasure.COSINE,
            5,
            10,
            true
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(
            xContentResult,
            is(
                """
                    {"model_id":"model","""
                    + """
                        "rate_limit":{"requests_per_minute":3},"similarity":"cosine","dimensions":5,"max_input_tokens":10,"""
                    + """
                        "embedding_type":"float","dimensions_set_by_user":true}"""
            )
        );
    }

    @Override
    protected Writeable.Reader<VoyageAITextEmbeddingServiceSettings> instanceReader() {
        return VoyageAITextEmbeddingServiceSettings::new;
    }

    @Override
    protected VoyageAITextEmbeddingServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAITextEmbeddingServiceSettings mutateInstance(VoyageAITextEmbeddingServiceSettings instance) throws IOException {
        var commonSettings = instance.getCommonSettings();
        var embeddingType = instance.getEmbeddingType();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        switch (randomInt(5)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, VoyageAIServiceSettingsTests::createRandom);
            case 1 -> embeddingType = randomValueOtherThan(embeddingType, () -> randomFrom(VoyageAIEmbeddingType.values()));
            case 2 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 5 -> dimensionsSetByUser = dimensionsSetByUser == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new VoyageAITextEmbeddingServiceSettings(
            commonSettings,
            embeddingType,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testUpdateEmbeddingDetails() {
        var settings = createRandom();
        var similarity = randomSimilarityMeasure();
        var dimensions = randomIntBetween(32, 256);

        var newSettings = settings.update(similarity, dimensions);

        var expectedSettings = new VoyageAITextEmbeddingServiceSettings(
            settings.getCommonSettings(),
            settings.getEmbeddingType(),
            similarity,
            dimensions,
            settings.maxInputTokens(),
            settings.dimensionsSetByUser()
        );

        assertThat(newSettings, is(expectedSettings));
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        return new HashMap<>(VoyageAIServiceSettingsTests.getServiceSettingsMap(model));
    }
}
