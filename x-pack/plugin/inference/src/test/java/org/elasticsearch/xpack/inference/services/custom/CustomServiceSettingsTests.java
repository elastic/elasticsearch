/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.is;

public class CustomServiceSettingsTests extends AbstractWireSerializingTestCase<CustomServiceSettings> {
    public static CustomServiceSettings createRandom(String inputUrl, String inputPath, String inputQueryString) {
        List<String> taskTypeStrList = Arrays.stream(TaskType.values()).map(TaskType::toString).toList();
        TaskType taskType = TaskType.fromString(randomFrom(taskTypeStrList));

        SimilarityMeasure similarityMeasure = null;
        Integer dims = null;
        var isTextEmbeddingModel = taskType.equals(TaskType.TEXT_EMBEDDING);
        if (isTextEmbeddingModel) {
            similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
            dims = 1536;
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        String description = randomAlphaOfLength(15);
        String version = randomAlphaOfLength(5);
        String url = inputUrl != null ? inputUrl : randomAlphaOfLength(15);
        String path = inputPath != null ? inputPath : randomAlphaOfLength(15);
        String method = randomFrom(HttpMethod.PUT.name(), HttpMethod.POST.name(), HttpMethod.GET.name());
        String queryString = inputQueryString != null ? inputQueryString : randomAlphaOfLength(15);
        Map<String, Object> headers = randomBoolean() ? null : Map.of("key", "value");
        Map<String, Object> requestContent = randomMap(0, 5, () -> tuple(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        String requestContentString = randomBoolean() ? null : randomAlphaOfLength(10);

        Map<String, Object> textEmbeddingJsonParserMap = new HashMap<>(
            Map.of(CustomServiceSettings.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
        );
        Map<String, Object> sparseEmbeddingJsonParserMap = new HashMap<>(
            Map.of(
                CustomServiceSettings.SPARSE_EMBEDDING_RESULT,
                new HashMap<>(
                    Map.of(
                        CustomServiceSettings.SPARSE_RESULT_PATH,
                        "$.result.sparse_embeddings[*]",
                        CustomServiceSettings.SPARSE_RESULT_VALUE,
                        new HashMap<>(
                            Map.of(
                                CustomServiceSettings.SPARSE_EMBEDDING_PARSER_TOKEN,
                                "$.embedding[*].token_id",
                                CustomServiceSettings.SPARSE_EMBEDDING_PARSER_WEIGHT,
                                "$.embedding[*].weights"
                            )
                        )
                    )
                )
            )
        );
        Map<String, Object> rerankJsonParserMap = new HashMap<>(
            Map.of(
                CustomServiceSettings.RERANK_PARSER_INDEX,
                "$.result.reranked_results[*].index",
                CustomServiceSettings.RERANK_PARSER_SCORE,
                "$.result.reranked_results[*].relevance_score",
                CustomServiceSettings.RERANK_PARSER_DOCUMENT_TEXT,
                "$.result.reranked_results[*].document_text"
            )
        );
        Map<String, Object> completionJsonParserMap = new HashMap<>(
            Map.of(CustomServiceSettings.COMPLETION_PARSER_RESULT, "$.result.text")
        );

        ResponseJsonParser responseJsonParser = switch (taskType) {
            case TEXT_EMBEDDING -> new ResponseJsonParser(taskType, textEmbeddingJsonParserMap, new ValidationException());
            case SPARSE_EMBEDDING -> new ResponseJsonParser(taskType, sparseEmbeddingJsonParserMap, new ValidationException());
            case RERANK -> new ResponseJsonParser(taskType, rerankJsonParserMap, new ValidationException());
            case COMPLETION -> new ResponseJsonParser(taskType, completionJsonParserMap, new ValidationException());
            default -> null;
        };

        RateLimitSettings rateLimitSettings = new RateLimitSettings(randomLongBetween(1, 1000000));

        return new CustomServiceSettings(
            similarityMeasure,
            dims,
            maxInputTokens,
            description,
            version,
            taskType.name(),
            url,
            path,
            method,
            queryString,
            headers,
            requestContent,
            requestContentString,
            responseJsonParser,
            rateLimitSettings
        );
    }

    public static CustomServiceSettings createRandom() {
        return createRandom(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testFromMap() {
        String similarity = SimilarityMeasure.DOT_PRODUCT.toString();
        Integer dims = 1536;
        Integer maxInputTokens = 512;
        String description = "test fromMap";
        String version = "v1";
        String serviceType = TaskType.TEXT_EMBEDDING.toString();
        String url = "http://www.abc.com";
        String path = "/endpoint";
        String method = HttpMethod.POST.name();
        String queryString = "?query=test";
        Map<String, Object> headers = Map.of("key", "value");
        String requestContentString = "request body";

        Map<String, Object> jsonParserMap = new HashMap<>(
            Map.of(CustomServiceSettings.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
        );
        ResponseJsonParser responseJsonParser = new ResponseJsonParser(TaskType.TEXT_EMBEDDING, jsonParserMap, new ValidationException());

        var settings = CustomServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.SIMILARITY,
                    similarity,
                    ServiceFields.DIMENSIONS,
                    dims,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens,
                    CustomServiceSettings.DESCRIPTION,
                    description,
                    CustomServiceSettings.VERSION,
                    version,
                    CustomServiceSettings.URL,
                    url,
                    CustomServiceSettings.PATH,
                    new HashMap<>(
                        Map.of(
                            path,
                            new HashMap<>(
                                Map.of(
                                    method,
                                    new HashMap<>(
                                        Map.of(
                                            CustomServiceSettings.QUERY_STRING,
                                            queryString,
                                            CustomServiceSettings.HEADERS,
                                            headers,
                                            CustomServiceSettings.REQUEST,
                                            new HashMap<>(Map.of(CustomServiceSettings.REQUEST_CONTENT, requestContentString)),
                                            CustomServiceSettings.RESPONSE,
                                            new HashMap<>(
                                                Map.of(
                                                    CustomServiceSettings.JSON_PARSER,
                                                    new HashMap<>(
                                                        Map.of(
                                                            CustomServiceSettings.TEXT_EMBEDDING_PARSER_EMBEDDINGS,
                                                            "$.result.embeddings[*].embedding"
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ),
            null,
            TaskType.TEXT_EMBEDDING
        );

        MatcherAssert.assertThat(
            settings,
            is(
                new CustomServiceSettings(
                    SimilarityMeasure.DOT_PRODUCT,
                    dims,
                    maxInputTokens,
                    description,
                    version,
                    serviceType,
                    url,
                    path,
                    method,
                    queryString,
                    headers,
                    null,
                    requestContentString,
                    responseJsonParser,
                    new RateLimitSettings(10_000)
                )
            )
        );
    }

    public void testXContent() throws IOException {
        Map<String, Object> jsonParserMap = new HashMap<>(
            Map.of(CustomServiceSettings.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
        );

        ResponseJsonParser responseJsonParser = new ResponseJsonParser(TaskType.TEXT_EMBEDDING, jsonParserMap, new ValidationException());

        var entity = new CustomServiceSettings(
            null,
            null,
            null,
            "test fromMap",
            "v1",
            TaskType.TEXT_EMBEDDING.toString(),
            "http://www.abc.com",
            "/endpoint",
            HttpMethod.POST.name(),
            "?query=test",
            Map.of("key", "value"),
            null,
            "request body",
            responseJsonParser,
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                "{\"description\":\"test fromMap\",\"version\":\"v1\","
                    + "\"url\":\"http://www.abc.com\",\"path\":{\"/endpoint\":{\"POST\":{\"query_string\":\"?query=test\","
                    + "\"headers\":{\"key\":\"value\"},\"request\":{\"content\":\"request body\"},"
                    + "\"response\":{\"json_parser\":{\"text_embeddings\":\"$.result.embeddings[*].embedding\"}}}}},"
                    + "\"rate_limit\":{\"requests_per_minute\":10000}}"
            )
        );
    }

    @Override
    protected Writeable.Reader<CustomServiceSettings> instanceReader() {
        return CustomServiceSettings::new;
    }

    @Override
    protected CustomServiceSettings createTestInstance() {
        return createRandom(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    @Override
    protected CustomServiceSettings mutateInstance(CustomServiceSettings instance) {
        return null;
    }
}
