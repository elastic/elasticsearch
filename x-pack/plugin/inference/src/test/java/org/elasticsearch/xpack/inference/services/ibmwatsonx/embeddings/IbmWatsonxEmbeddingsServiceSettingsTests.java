/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<IbmWatsonxEmbeddingsServiceSettings> {

    private static IbmWatsonxEmbeddingsServiceSettings createRandom() {
        URI uri = null;
        try {
            uri = new URI("http://abc.com");
        } catch (Exception ignored) {}

        return new IbmWatsonxEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            uri,
            randomAlphaOfLength(8),
            randomFrom(randomNonNegativeInt(), null),
            randomFrom(randomNonNegativeInt(), null),
            randomFrom(randomSimilarityMeasure(), null),
            randomFrom(RateLimitSettingsTests.createRandom(), null)
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var model = randomAlphaOfLength(8);
        var projectId = randomAlphaOfLength(8);
        URI uri = null;
        try {
            uri = new URI("http://abc.com");
        } catch (Exception ignored) {}
        var apiVersion = randomAlphaOfLength(8);
        var maxInputTokens = randomIntBetween(1, 1024);
        var dims = randomIntBetween(1, 10000);
        var similarity = randomSimilarityMeasure();

        var serviceSettings = IbmWatsonxEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    IbmWatsonxServiceFields.PROJECT_ID,
                    projectId,
                    ServiceFields.URL,
                    uri.toString(),
                    IbmWatsonxServiceFields.API_VERSION,
                    apiVersion,
                    ServiceFields.MODEL_ID,
                    model,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.SIMILARITY,
                    similarity.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new IbmWatsonxEmbeddingsServiceSettings(model, projectId, uri, apiVersion, maxInputTokens, dims, similarity, null))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        URI uri = null;
        try {
            uri = new URI("https://abc.com");
        } catch (Exception ignored) {}
        var entity = new IbmWatsonxEmbeddingsServiceSettings(
            "model",
            "project_id",
            uri,
            "2024-05-02",
            1024,
            8,
            SimilarityMeasure.DOT_PRODUCT,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "project_id":"project_id",
                "url":"https://abc.com",
                "api_version":"2024-05-02",
                "max_input_tokens": 1024,
                "dimensions": 8,
                "similarity": "dot_product",
                "rate_limit": {
                    "requests_per_minute":120
                }
            }"""));
    }

    @Override
    protected Writeable.Reader<IbmWatsonxEmbeddingsServiceSettings> instanceReader() {
        return IbmWatsonxEmbeddingsServiceSettings::new;
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected IbmWatsonxEmbeddingsServiceSettings mutateInstance(IbmWatsonxEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, IbmWatsonxEmbeddingsServiceSettingsTests::createRandom);
    }
}
