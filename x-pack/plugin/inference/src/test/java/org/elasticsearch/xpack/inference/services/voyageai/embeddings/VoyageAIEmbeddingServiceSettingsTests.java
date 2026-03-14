/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.PERSISTENT;
import static org.elasticsearch.xpack.inference.services.ConfigurationParseContext.REQUEST;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.hamcrest.Matchers.is;

public class VoyageAIEmbeddingServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAIEmbeddingServiceSettings> {

    private static final TransportVersion VOYAGE_AI_MULTIMODAL_EMBEDDINGS_ADDED = TransportVersion.fromName(
        "voyage_ai_multimodal_embeddings_added"
    );

    public static VoyageAIEmbeddingServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = randomBoolean() ? null : randomSimilarityMeasure();
        Integer dimensions = randomBoolean() ? null : randomIntBetween(32, 256);
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = VoyageAIServiceSettingsTests.createRandom();
        var embeddingType = randomBoolean() ? null : randomFrom(VoyageAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();
        var multimodalModel = randomBoolean();

        return new VoyageAIEmbeddingServiceSettings(
            commonSettings,
            embeddingType,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    public static VoyageAIEmbeddingServiceSettings createRandomWithNoNullValues() {
        SimilarityMeasure similarityMeasure = randomSimilarityMeasure();
        Integer dimensions = randomIntBetween(32, 256);
        Integer maxInputTokens = randomIntBetween(128, 256);

        var commonSettings = VoyageAIServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(VoyageAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();
        var multimodalModel = randomBoolean();

        return new VoyageAIEmbeddingServiceSettings(
            commonSettings,
            embeddingType,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    @Override
    protected Writeable.Reader<VoyageAIEmbeddingServiceSettings> instanceReader() {
        return VoyageAIEmbeddingServiceSettings::new;
    }

    @Override
    protected VoyageAIEmbeddingServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIEmbeddingServiceSettings mutateInstance(VoyageAIEmbeddingServiceSettings instance) throws IOException {
        var commonSettings = instance.getCommonSettings();
        var embeddingType = instance.getEmbeddingType();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        var multimodal = instance.isMultimodal();
        switch (randomInt(6)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, VoyageAIServiceSettingsTests::createRandom);
            case 1 -> embeddingType = randomValueOtherThan(embeddingType, () -> randomFrom(VoyageAIEmbeddingType.values()));
            case 2 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 3 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 5 -> dimensionsSetByUser = randomValueOtherThan(dimensionsSetByUser, ESTestCase::randomBoolean);
            case 6 -> multimodal = randomValueOtherThan(multimodal, ESTestCase::randomBoolean);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new VoyageAIEmbeddingServiceSettings(
            commonSettings,
            embeddingType,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            multimodal
        );
    }

    @Override
    protected VoyageAIEmbeddingServiceSettings mutateInstanceForVersion(VoyageAIEmbeddingServiceSettings instance, TransportVersion version) {
        boolean multimodalModel = instance.isMultimodal();

        if (version.supports(VOYAGE_AI_MULTIMODAL_EMBEDDINGS_ADDED) == false) {
            multimodalModel = false;
        }

        return new VoyageAIEmbeddingServiceSettings(
            instance.getCommonSettings(),
            instance.getEmbeddingType(),
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            instance.dimensionsSetByUser(),
            multimodalModel
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testFromMap() {
        var similarity = randomSimilarityMeasure();
        var dimensions = randomNonNegativeInt();
        var maxInputTokens = randomNonNegativeInt();
        var model = randomAlphanumericOfLength(8);
        var embeddingType = randomFrom(VoyageAIEmbeddingType.values());
        var multimodalModel = randomBoolean();

        var settingsMap = getServiceSettingsMap(model, multimodalModel);
        settingsMap.put(ServiceFields.SIMILARITY, similarity.toString());
        settingsMap.put(DIMENSIONS, dimensions);
        settingsMap.put(MAX_INPUT_TOKENS, maxInputTokens);
        settingsMap.put(EMBEDDING_TYPE, embeddingType.toString());

        var dimensionsSetByUser = randomBoolean();
        settingsMap.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        var serviceSettings = VoyageAIEmbeddingServiceSettings.fromMap(settingsMap, PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    embeddingType,
                    similarity,
                    dimensions,
                    maxInputTokens,
                    dimensionsSetByUser,
                    multimodalModel
                )
            )
        );
    }

    public void testFromMap_RequestContext() {
        var similarity = randomSimilarityMeasure();
        var dimensions = randomNonNegativeInt();
        var maxInputTokens = randomNonNegativeInt();
        var model = randomAlphanumericOfLength(8);
        var embeddingType = randomFrom(VoyageAIEmbeddingType.values());
        var multimodalModel = randomBoolean();

        var settingsMap = getServiceSettingsMap(model, multimodalModel);
        settingsMap.put(ServiceFields.SIMILARITY, similarity.toString());
        settingsMap.put(DIMENSIONS, dimensions);
        settingsMap.put(MAX_INPUT_TOKENS, maxInputTokens);
        settingsMap.put(EMBEDDING_TYPE, embeddingType.toString());

        var serviceSettings = VoyageAIEmbeddingServiceSettings.fromMap(settingsMap, REQUEST);

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    embeddingType,
                    similarity,
                    dimensions,
                    maxInputTokens,
                    true,
                    multimodalModel
                )
            )
        );
    }

    public void testFromMap_DefaultsMultimodalToTrue() {
        var model = "model";
        var serviceSettings = VoyageAIEmbeddingServiceSettings.fromMap(
            new HashMap<>(Map.of(MODEL_ID, model)),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new VoyageAIEmbeddingServiceSettings(
                    new VoyageAIServiceSettings(model, null),
                    VoyageAIEmbeddingType.FLOAT,
                    null,
                    null,
                    null,
                    false,
                    true
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new VoyageAIEmbeddingServiceSettings(
            new VoyageAIServiceSettings("model", new RateLimitSettings(3)),
            VoyageAIEmbeddingType.FLOAT,
            SimilarityMeasure.COSINE,
            5,
            10,
            true,
            true
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is(stripWhitespace("""
            {
                "model_id":"model",
                "rate_limit":{"requests_per_minute":3},
                "similarity":"cosine",
                "dimensions":5,
                "max_input_tokens":10,
                "embedding_type":"float",
                "multimodal_model":true,
                "dimensions_set_by_user":true
            }""")));
    }

    public void testUpdateEmbeddingDetails() {
        var settings = createRandomWithNoNullValues();
        var similarity = randomSimilarityMeasure();
        var dimensions = randomIntBetween(32, 256);

        var newSettings = settings.update(similarity, dimensions);

        var expectedSettings = new VoyageAIEmbeddingServiceSettings(
            settings.getCommonSettings(),
            settings.getEmbeddingType(),
            similarity,
            dimensions,
            settings.maxInputTokens(),
            settings.dimensionsSetByUser(),
            settings.isMultimodal()
        );

        assertThat(newSettings, is(expectedSettings));
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        return new HashMap<>(VoyageAIServiceSettingsTests.getServiceSettingsMap(model));
    }

    public static Map<String, Object> getServiceSettingsMap(String model, boolean multimodalModel) {
        var map = getServiceSettingsMap(model);
        map.put(MULTIMODAL_MODEL, multimodalModel);
        return map;
    }
}
