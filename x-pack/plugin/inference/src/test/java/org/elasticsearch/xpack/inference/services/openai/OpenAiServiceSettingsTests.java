/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.common.SimilarityMeasure;
import org.elasticsearch.xpack.inference.services.ServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenAiServiceSettingsTests extends AbstractWireSerializingTestCase<OpenAiServiceSettings> {

    public static OpenAiServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    /**
     * The created settings can have a url set to null.
     */
    public static OpenAiServiceSettings createRandom() {
        var url = randomBoolean() ? randomAlphaOfLength(15) : null;
        return createRandom(url);
    }

    private static OpenAiServiceSettings createRandom(String url) {
        var organizationId = randomBoolean() ? randomAlphaOfLength(15) : null;
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        return new OpenAiServiceSettings(url, organizationId, similarityMeasure, dims);
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var org = "organization";
        var similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        var dims = 1536;
        var serviceSettings = OpenAiServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    OpenAiServiceSettings.ORGANIZATION,
                    org,
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims
                )
            )
        );

        assertThat(serviceSettings, is(new OpenAiServiceSettings(url, org, SimilarityMeasure.DOT_PRODUCT, dims)));
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var serviceSettings = OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceSettings.ORGANIZATION, "org")));
        assertNull(serviceSettings.uri());
        assertThat(serviceSettings.organizationId(), is("org"));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")))
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
        var serviceSettings = OpenAiServiceSettings.fromMap(new HashMap<>());
        assertNull(serviceSettings.uri());
        assertNull(serviceSettings.organizationId());
    }

    public void testFromMap_EmptyOrganization_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceSettings.ORGANIZATION, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    OpenAiServiceSettings.ORGANIZATION
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)))
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
            () -> OpenAiServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.SIMILARITY, similarity)))
        );

        assertThat(thrownException.getMessage(), is("Validation Failed: 1: [service_settings] Unknown similarity measure [by_size];"));
    }

    @Override
    protected Writeable.Reader<OpenAiServiceSettings> instanceReader() {
        return OpenAiServiceSettings::new;
    }

    @Override
    protected OpenAiServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected OpenAiServiceSettings mutateInstance(OpenAiServiceSettings instance) throws IOException {
        return createRandomWithNonNullUrl();
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String org) {
        var map = new HashMap<String, Object>();

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        if (org != null) {
            map.put(OpenAiServiceSettings.ORGANIZATION, org);
        }
        return map;
    }
}
