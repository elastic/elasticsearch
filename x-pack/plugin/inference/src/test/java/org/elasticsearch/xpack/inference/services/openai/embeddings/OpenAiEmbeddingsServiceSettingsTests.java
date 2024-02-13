/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.SimilarityMeasure;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiParseContext;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<OpenAiEmbeddingsServiceSettings> {

    public static OpenAiEmbeddingsServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    /**
     * The created settings can have a url set to null.
     */
    public static OpenAiEmbeddingsServiceSettings createRandom() {
        var url = randomBoolean() ? randomAlphaOfLength(15) : null;
        return createRandom(url);
    }

    private static OpenAiEmbeddingsServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);
        var organizationId = randomBoolean() ? randomAlphaOfLength(15) : null;
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        return new OpenAiEmbeddingsServiceSettings(
            modelId,
            ServiceUtils.createUri(url),
            organizationId,
            similarityMeasure,
            dims,
            maxInputTokens,
            randomBoolean()
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var org = "organization";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    OpenAiEmbeddingsServiceSettings.ORGANIZATION,
                    org,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            OpenAiParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiEmbeddingsServiceSettings(
                    modelId,
                    ServiceUtils.createUri(url),
                    org,
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    true
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var org = "organization";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var maxInputTokens = 512;
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    OpenAiEmbeddingsServiceSettings.ORGANIZATION,
                    org,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            OpenAiParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiEmbeddingsServiceSettings(
                    modelId,
                    ServiceUtils.createUri(url),
                    org,
                    SimilarityMeasure.DOT_PRODUCT,
                    null,
                    maxInputTokens,
                    false
                )
            )
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var modelId = "model-foo";
        var url = "https://www.abc.com";
        var org = "organization";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var maxInputTokens = 512;
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    OpenAiEmbeddingsServiceSettings.ORGANIZATION,
                    org,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    OpenAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                    false
                )
            ),
            OpenAiParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiEmbeddingsServiceSettings(
                    modelId,
                    ServiceUtils.createUri(url),
                    org,
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    false
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var settings = OpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, true, ServiceFields.MODEL_ID, "m")),
            OpenAiParseContext.PERSISTENT
        );

        assertThat(settings, is(new OpenAiEmbeddingsServiceSettings("m", (URI) null, null, null, null, null, true)));
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var exception = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.DIMENSIONS, 1, ServiceFields.MODEL_ID, "m")),
                OpenAiParseContext.PERSISTENT
            )
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "m", OpenAiEmbeddingsServiceSettings.ORGANIZATION, "org")),
            OpenAiParseContext.REQUEST
        );
        assertNull(serviceSettings.uri());
        assertThat(serviceSettings.modelId(), is("m"));
        assertThat(serviceSettings.organizationId(), is("org"));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, "", ServiceFields.MODEL_ID, "m")),
                OpenAiParseContext.REQUEST
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

    public void testFromMap_MissingOrganization_DoesNotThrowException() {
        var serviceSettings = OpenAiEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "m")),
            OpenAiParseContext.REQUEST
        );
        assertNull(serviceSettings.uri());
        assertNull(serviceSettings.organizationId());
    }

    public void testFromMap_EmptyOrganization_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(OpenAiEmbeddingsServiceSettings.ORGANIZATION, "", ServiceFields.MODEL_ID, "m")),
                OpenAiParseContext.REQUEST
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    OpenAiEmbeddingsServiceSettings.ORGANIZATION
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, url, ServiceFields.MODEL_ID, "m")),
                OpenAiParseContext.REQUEST
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s];", url, ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity, ServiceFields.MODEL_ID, "m")),
                OpenAiParseContext.REQUEST
            )
        );

        assertThat(thrownException.getMessage(), is("Validation Failed: 1: [service_settings] Unknown similarity measure [by_size];"));
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings("model", "url", "org", null, null, null, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model_id":"model","url":"url","organization_id":"org","dimensions_set_by_user":true}"""));
    }

    public void testToXContent_WritesDimensionsSetByUserFalse() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings("model", "url", "org", null, null, null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model_id":"model","url":"url","organization_id":"org","dimensions_set_by_user":false}"""));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings("model", "url", "org", SimilarityMeasure.DOT_PRODUCT, 1, 2, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model_id":"model","url":"url","organization_id":"org","similarity":"dot_product",""" + """
            "dimensions":1,"max_input_tokens":2,"dimensions_set_by_user":false}"""));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new OpenAiEmbeddingsServiceSettings("model", "url", "org", SimilarityMeasure.DOT_PRODUCT, 1, 2, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model_id":"model","url":"url","organization_id":"org","similarity":"dot_product",""" + """
            "dimensions":1,"max_input_tokens":2}"""));
    }

    @Override
    protected Writeable.Reader<OpenAiEmbeddingsServiceSettings> instanceReader() {
        return OpenAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected OpenAiEmbeddingsServiceSettings mutateInstance(OpenAiEmbeddingsServiceSettings instance) throws IOException {
        return createRandomWithNonNullUrl();
    }

    public static Map<String, Object> getServiceSettingsMap(String modelId, @Nullable String url, @Nullable String org) {
        var map = new HashMap<String, Object>();
        map.put(ServiceFields.MODEL_ID, modelId);
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (org != null) {
            map.put(OpenAiEmbeddingsServiceSettings.ORGANIZATION, org);
        }
        return map;
    }

    public static Map<String, Object> getServiceSettingsMap(
        String model,
        @Nullable String url,
        @Nullable String org,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser
    ) {
        var map = new HashMap<String, Object>();
        map.put(ServiceFields.MODEL_ID, model);

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (org != null) {
            map.put(OpenAiEmbeddingsServiceSettings.ORGANIZATION, org);
        }

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(OpenAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        return map;
    }
}
