/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.NoopResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.ResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CustomServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CustomServiceSettings> {
    public static CustomServiceSettings createRandom(String inputUrl) {
        var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.SPARSE_EMBEDDING, TaskType.COMPLETION);

        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = taskType.equals(TaskType.TEXT_EMBEDDING);
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        String url = inputUrl != null ? inputUrl : randomAlphaOfLength(15);
        Map<String, String> headers = randomBoolean() ? Map.of() : Map.of("key", "value");
        String requestContentString = randomAlphaOfLength(10);

        ResponseParser responseJsonParser = switch (taskType) {
            case TEXT_EMBEDDING -> new TextEmbeddingResponseParser("$.result.embeddings[*].embedding");
            case SPARSE_EMBEDDING -> new SparseEmbeddingResponseParser(
                "$.result.sparse_embeddings[*]",
                "$.embedding[*].token_id",
                "$.embedding[*].weights"
            );
            case RERANK -> new RerankResponseParser(
                "$.result.reranked_results[*].index",
                "$.result.reranked_results[*].relevance_score",
                "$.result.reranked_results[*].document_text"
            );
            case COMPLETION -> new CompletionResponseParser("$.result.text");
            default -> new NoopResponseParser();
        };

        var errorParser = new ErrorResponseParser("$.error.message");

        RateLimitSettings rateLimitSettings = new RateLimitSettings(randomLongBetween(1, 1000000));

        return new CustomServiceSettings(
            similarityMeasure,
            dims,
            maxInputTokens,
            url,
            headers,
            requestContentString,
            responseJsonParser,
            rateLimitSettings,
            errorParser
        );
    }

    public static CustomServiceSettings createRandom() {
        return createRandom(randomAlphaOfLength(5));
    }

    public void testFromMap() {
        String similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        Integer dims = 1536;
        Integer maxInputTokens = 512;
        String url = "http://www.abc.com";
        Map<String, String> headers = Map.of("key", "value");
        String requestContentString = "request body";

        var responseParser = new TextEmbeddingResponseParser("$.result.embeddings[*].embedding");

        var settings = CustomServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CustomServiceSettings.URL,
                    url,
                    CustomServiceSettings.HEADERS,
                    headers,
                    CustomServiceSettings.REQUEST,
                    new HashMap<>(Map.of(CustomServiceSettings.REQUEST_CONTENT, requestContentString)),
                    CustomServiceSettings.RESPONSE,
                    new HashMap<>(
                        Map.of(
                            CustomServiceSettings.JSON_PARSER,
                            new HashMap<>(
                                Map.of(TextEmbeddingResponseParser.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
                            )
                        )
                    )
                )
            ),
            ConfigurationParseContext.REQUEST,
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(
            settings,
            is(
                new CustomServiceSettings(
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    url,
                    headers,
                    requestContentString,
                    responseParser,
                    new RateLimitSettings(10_000),
                    new ErrorResponseParser("$.error.message")
                )
            )
        );
    }

    public void testXContent() throws IOException {
        var entity = new CustomServiceSettings(
            null,
            null,
            null,
            "http://www.abc.com",
            Map.of("key", "value"),
            "string",
            new TextEmbeddingResponseParser("$.result.embeddings[*].embedding"),
            null,
            new ErrorResponseParser("$.error.message")
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "url": "http://www.abc.com",
                "headers": {
                    "key": "value"
                },
                "request": {
                    "content": "string"
                },
                "response": {
                    "json_parser": {
                        "text_embeddings": "$.result.embeddings[*].embedding"
                    },
                    "error_parser": {
                        "path": "$.error.message"
                    }
                },
                "rate_limit": {
                    "requests_per_minute": 10000
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<CustomServiceSettings> instanceReader() {
        return CustomServiceSettings::new;
    }

    @Override
    protected CustomServiceSettings createTestInstance() {
        return createRandom(randomAlphaOfLength(5));
    }

    @Override
    protected CustomServiceSettings mutateInstance(CustomServiceSettings instance) {
        return randomValueOtherThan(instance, CustomServiceSettingsTests::createRandom);
    }

    @Override
    protected CustomServiceSettings mutateInstanceForVersion(CustomServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
