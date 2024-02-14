/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HuggingFaceServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceServiceSettings> {

    public static HuggingFaceServiceSettings createRandom() {
        return createRandom(randomAlphaOfLength(15));
    }

    private static HuggingFaceServiceSettings createRandom(String url) {
        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = randomFrom(SimilarityMeasure.values());
            dims = randomIntBetween(32, 256);
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        return new HuggingFaceServiceSettings(ServiceUtils.createUri(url), similarityMeasure, dims, maxInputTokens);
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dims = 384;
        var maxInputTokens = 128;
        {
            var serviceSettings = HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)));
            assertThat(serviceSettings, is(new HuggingFaceServiceSettings(url)));
        }
        {
            var serviceSettings = HuggingFaceServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        url,
                        ServiceFields.SIMILARITY,
                        similarity.toString(),
                        ServiceFields.DIMENSIONS,
                        dims,
                        ServiceFields.MAX_INPUT_TOKENS,
                        maxInputTokens
                    )
                )
            );
            assertThat(serviceSettings, is(new HuggingFaceServiceSettings(ServiceUtils.createUri(url), similarity, dims, maxInputTokens)));
        }
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> HuggingFaceServiceSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] does not contain the required setting [%s];", ServiceFields.URL)
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")))
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
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s];", url, ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var url = "https://www.abc.com";
        var similarity = "by_size";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url, ServiceFields.SIMILARITY, similarity)))
        );

        assertThat(thrownException.getMessage(), is("Validation Failed: 1: [service_settings] Unknown similarity measure [by_size];"));
    }

    @Override
    protected Writeable.Reader<HuggingFaceServiceSettings> instanceReader() {
        return HuggingFaceServiceSettings::new;
    }

    @Override
    protected HuggingFaceServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceServiceSettings mutateInstance(HuggingFaceServiceSettings instance) throws IOException {
        return createRandom();
    }

    public static Map<String, Object> getServiceSettingsMap(String url) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.URL, url);

        return map;
    }
}
