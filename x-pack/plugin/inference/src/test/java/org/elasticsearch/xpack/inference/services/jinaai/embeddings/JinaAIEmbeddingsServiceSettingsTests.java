/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsServiceSettings.JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED;
import static org.elasticsearch.xpack.inference.services.settings.RateLimitSettings.REQUESTS_PER_MINUTE_FIELD;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIEmbeddingsServiceSettings> {

    private static final TransportVersion JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED = TransportVersion.fromName(
        "jina_ai_embedding_type_support_added"
    );

    public static JinaAIEmbeddingsServiceSettings createRandom() {
        SimilarityMeasure similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
        Integer dimensions = 1024;
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);

        var commonSettings = JinaAIServiceSettingsTests.createRandom();
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = randomBoolean();

        return new JinaAIEmbeddingsServiceSettings(
            commonSettings,
            similarityMeasure,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dimensions = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var requestsPerMinute = 1234;
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity.toString(),
                    ServiceFields.DIMENSIONS,
                    dimensions,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model,
                    EMBEDDING_TYPE,
                    embeddingType.toString(),
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(REQUESTS_PER_MINUTE_FIELD, requestsPerMinute))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
                    similarity,
                    dimensions,
                    maxInputTokens,
                    embeddingType,
                    true
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var url = "https://www.abc.com";
        var model = "model";
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(URL, url, ServiceFields.MODEL_ID, model)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(model, null),
                    null,
                    null,
                    null,
                    JinaAIEmbeddingType.FLOAT,
                    false
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var url = "https://www.abc.com";
        var similarity = randomSimilarityMeasure();
        var dimensions = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var requestsPerMinute = 1234;
        var dimensionsSetByUser = randomBoolean();
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    URL,
                    url,
                    SIMILARITY,
                    similarity.toString(),
                    DIMENSIONS,
                    dimensions,
                    MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model,
                    EMBEDDING_TYPE,
                    embeddingType.toString(),
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(REQUESTS_PER_MINUTE_FIELD, requestsPerMinute)),
                    ServiceFields.DIMENSIONS_SET_BY_USER,
                    dimensionsSetByUser
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(model, new RateLimitSettings(requestsPerMinute)),
                    similarity,
                    dimensions,
                    maxInputTokens,
                    embeddingType,
                    dimensionsSetByUser
                )
            )
        );
    }

    public void testFromMap_WithModelId() {
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dimensions = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity.toString(),
                    DIMENSIONS,
                    dimensions,
                    MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(model, null),
                    similarity,
                    dimensions,
                    maxInputTokens,
                    JinaAIEmbeddingType.FLOAT,
                    true
                )
            )
        );
    }

    public void testFromMap_WithEmbeddingType() {
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dimensions = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = JinaAIEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity.toString(),
                    DIMENSIONS,
                    dimensions,
                    MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.MODEL_ID,
                    model,
                    EMBEDDING_TYPE,
                    JinaAIEmbeddingType.BIT.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new JinaAIEmbeddingsServiceSettings(
                    new JinaAIServiceSettings(model, null),
                    similarity,
                    dimensions,
                    maxInputTokens,
                    JinaAIEmbeddingType.BIT,
                    true
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", SIMILARITY, similarity)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. [similarity] "
                    + "must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    public void testFromMap_nonPositiveDimensions_ThrowsError() {
        var dimensions = randomIntBetween(-5, 0);
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model", DIMENSIONS, dimensions)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%d]. [%s] must be a positive integer;",
                    dimensions,
                    DIMENSIONS
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var modelName = randomAlphanumericOfLength(10);
        var requestsPerMinute = randomNonNegativeInt();
        var similarity = randomSimilarityMeasure();
        var dimensions = randomNonNegativeInt();
        var maxInputTokens = randomNonNegativeInt();
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = false;
        var serviceSettings = new JinaAIEmbeddingsServiceSettings(
            new JinaAIServiceSettings(modelName, new RateLimitSettings(requestsPerMinute)),
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
                "model_id":"%s",
                "rate_limit":{"requests_per_minute":%d},
                "dimensions":%d,
                "embedding_type":"%s",
                "max_input_tokens":%d,
                "similarity":"%s",
                "dimensions_set_by_user":%b
            }""", modelName, requestsPerMinute, dimensions, embeddingType, maxInputTokens, similarity, dimensionsSetByUser))));
    }

    public void testToXContentFragmentOfExposedFields_WritesAllValues() throws IOException {
        var modelName = randomAlphanumericOfLength(10);
        var requestsPerMinute = randomNonNegativeInt();
        var similarity = randomSimilarityMeasure();
        var dimensions = randomNonNegativeInt();
        var maxInputTokens = randomNonNegativeInt();
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var dimensionsSetByUser = false;
        var serviceSettings = new JinaAIEmbeddingsServiceSettings(
            new JinaAIServiceSettings(modelName, new RateLimitSettings(requestsPerMinute)),
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is(stripWhitespace(Strings.format("""
            {
                "model_id":"%s",
                "rate_limit":{"requests_per_minute":%d},
                "dimensions":%d,
                "embedding_type":"%s",
                "max_input_tokens":%d,
                "similarity":"%s"
            }""", modelName, requestsPerMinute, dimensions, embeddingType, maxInputTokens, similarity))));
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
        var commonSettings = instance.getCommonSettings();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var embeddingType = instance.getEmbeddingType();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        switch (randomInt(5)) {
            case 0 -> commonSettings = randomValueOtherThan(commonSettings, JinaAIServiceSettingsTests::createRandom);
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 2 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 4 -> embeddingType = randomValueOtherThan(embeddingType, () -> randomFrom(JinaAIEmbeddingType.values()));
            case 5 -> dimensionsSetByUser = dimensionsSetByUser == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAIEmbeddingsServiceSettings(
            commonSettings,
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
    }

    @Override
    protected JinaAIEmbeddingsServiceSettings mutateInstanceForVersion(JinaAIEmbeddingsServiceSettings instance, TransportVersion version) {
        if (version.supports(JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED)) {
            return instance;
        }

        JinaAIEmbeddingType embeddingType;
        if (version.supports(JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)) {
            embeddingType = instance.getEmbeddingType();
        } else {
            // default to null embedding type if node is on a version before embedding type was introduced
            embeddingType = null;
        }

        return new JinaAIEmbeddingsServiceSettings(
            instance.getCommonSettings(),
            instance.similarity(),
            instance.dimensions(),
            instance.maxInputTokens(),
            embeddingType,
            false
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(String model, @Nullable JinaAIEmbeddingType embeddingType) {
        var map = new HashMap<>(JinaAIServiceSettingsTests.getServiceSettingsMap(model));

        if (embeddingType != null) {
            map.put(EMBEDDING_TYPE, embeddingType.toString());
        }

        return map;
    }
}
