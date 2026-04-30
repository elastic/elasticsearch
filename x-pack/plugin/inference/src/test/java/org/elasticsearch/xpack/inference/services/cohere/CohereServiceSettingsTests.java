/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereServiceSettings> {

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");
    private static final String TEST_URL = "https://www.test.com";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final String INVALID_TEST_URL = "https://www.abc^.com";

    public static CohereServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    public static CohereServiceSettings createRandom() {
        return createRandom(randomAlphaOfLengthOrNull(15));
    }

    private static CohereServiceSettings createRandom(String url) {
        SimilarityMeasure similarityMeasure = null;
        Integer dimensions = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dimensions = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var model = randomAlphaOfLengthOrNull(15);

        return new CohereServiceSettings(
            ServiceUtils.createOptionalUri(url),
            similarityMeasure,
            dimensions,
            maxInputTokens,
            model,
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereServiceSettings.CohereApiVersion.values())
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, "")),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, INVALID_TEST_URL)),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]",
                    INVALID_TEST_URL,
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity)),
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

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereServiceSettings(
            TEST_URL,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V2
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
                              "api_version": "%s"
                            }
                            """,
                        TEST_URL,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS,
                        TEST_MODEL_ID,
                        TEST_RATE_LIMIT,
                        CohereServiceSettings.CohereApiVersion.V2
                    )
                )
            )
        );
    }

    public void testToXContentFragmentOfExposedFields_DoesNotWriteApiVersion() throws IOException {
        var serviceSettings = new CohereServiceSettings(
            TEST_URL,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_MODEL_ID,
            new RateLimitSettings(TEST_RATE_LIMIT),
            CohereServiceSettings.CohereApiVersion.V2
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "url": "%s",
              "similarity": "%s",
              "dimensions": %d,
              "max_input_tokens": %d,
              "model_id": "%s",
              "rate_limit": {
                "requests_per_minute": %d
              }
            }
            """, TEST_URL, TEST_SIMILARITY_MEASURE, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, TEST_MODEL_ID, TEST_RATE_LIMIT))));
    }

    @Override
    protected Writeable.Reader<CohereServiceSettings> instanceReader() {
        return CohereServiceSettings::new;
    }

    @Override
    protected CohereServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected CohereServiceSettings mutateInstance(CohereServiceSettings instance) throws IOException {
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();
        switch (randomInt(6)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(15));
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 2 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 4 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(15));
            case 5 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 6 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomFrom(CohereServiceSettings.CohereApiVersion.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereServiceSettings(uriString, similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        var serviceSettingsMap = new HashMap<String, Object>();

        if (url != null) {
            serviceSettingsMap.put(ServiceFields.URL, url);
        }

        if (model != null) {
            serviceSettingsMap.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, model);
        }

        return serviceSettingsMap;
    }

    @Override
    protected CohereServiceSettings mutateInstanceForVersion(CohereServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereServiceSettings(
                instance.uri(),
                instance.similarity(),
                instance.dimensions(),
                instance.maxInputTokens(),
                instance.modelId(),
                instance.rateLimitSettings(),
                CohereServiceSettings.CohereApiVersion.V1
            );
        }

        return instance;
    }
}
