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
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.hamcrest.CoreMatchers;
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

        return new CohereServiceSettings(ServiceUtils.createOptionalUri(url), similarityMeasure, dims, maxInputTokens, model);
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
            true
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(new CohereServiceSettings(ServiceUtils.createUri(url), SimilarityMeasure.DOT_PRODUCT, dims, maxInputTokens, model))
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
                    model
                )
            ),
            false
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(new CohereServiceSettings(ServiceUtils.createUri(url), SimilarityMeasure.DOT_PRODUCT, dims, maxInputTokens, model))
        );
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
            false
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(new CohereServiceSettings(ServiceUtils.createUri(url), SimilarityMeasure.DOT_PRODUCT, dims, maxInputTokens, model))
        );
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var serviceSettings = CohereServiceSettings.fromMap(new HashMap<>(Map.of()), false);
        assertNull(serviceSettings.getUri());
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")), false)
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
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)), false)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s];", url, ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity)), false)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Validation Failed: 1: [service_settings] Unknown similarity measure [by_size];")
        );
    }

    public void testXContent_WritesModelId() throws IOException {
        var entity = new CohereServiceSettings((String) null, null, null, null, "modelId");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model_id":"modelId"}"""));
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
        return null;
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
