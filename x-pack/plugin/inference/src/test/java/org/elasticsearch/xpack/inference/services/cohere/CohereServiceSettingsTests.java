/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereServiceSettingsTests extends AbstractWireSerializingTestCase<CohereServiceSettings> {

    public static CohereServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    /**
     * The created settings can have a url set to null.
     */
    public static CohereServiceSettings createRandom() {
        var url = randomBoolean() ? randomAlphaOfLength(15) : null;
        return createRandom(url);
    }

    private static CohereServiceSettings createRandom(String url) {
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        var model = randomBoolean() ? randomAlphaOfLength(15) : null;

        return new CohereServiceSettings(
            ServiceUtils.createOptionalUri(url),
            similarityMeasure,
            dims,
            maxInputTokens,
            model,
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereServiceSettings.CohereApiVersion.values())
        );
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
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
                    model
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    null,
                    CohereServiceSettings.CohereApiVersion.V2
                )
            )
        );
    }

    public void testFromMap_WithRateLimit() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
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
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    new RateLimitSettings(3),
                    CohereServiceSettings.CohereApiVersion.V2
                )
            )
        );
    }

    public void testFromMap_WhenUsingModelId() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
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
                    CohereServiceSettings.MODEL_ID,
                    model,
                    CohereServiceSettings.API_VERSION,
                    CohereServiceSettings.CohereApiVersion.V1.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    null,
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testFromMap_MissingModelId() {
        var e = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.DOT_PRODUCT.toString(),
                        ServiceFields.DIMENSIONS,
                        1536,
                        ServiceFields.MAX_INPUT_TOKENS,
                        512
                    )
                ),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(e.validationErrors().get(0), containsString("The [service_settings.model_id] field is required for the Cohere V2 API."));
    }

    public void testFromMap_PrefersModelId_OverModel() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var model = "model";
        var serviceSettings = CohereServiceSettings.fromMap(
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
                    model
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new CohereServiceSettings(
                    ServiceUtils.createUri(url),
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    model,
                    null,
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var serviceSettings = CohereServiceSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertNull(serviceSettings.uri());
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
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
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]", url, ServiceFields.URL)
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

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. [similarity] "
                    + "must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    public void testXContent_WritesModelId() throws IOException {
        var entity = new CohereServiceSettings(
            (String) null,
            null,
            null,
            null,
            "modelId",
            new RateLimitSettings(1),
            CohereServiceSettings.CohereApiVersion.V2
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"modelId","rate_limit":{"requests_per_minute":1},"api_version":"V2"}"""));
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
        return randomValueOtherThan(instance, CohereServiceSettingsTests::createRandom);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        var map = new HashMap<String, Object>();

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (model != null) {
            map.put(CohereServiceSettings.OLD_MODEL_ID_FIELD, model);
        }

        return map;
    }
}
