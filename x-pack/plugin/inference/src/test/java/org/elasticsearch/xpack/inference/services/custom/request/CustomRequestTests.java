/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.custom.CustomModelTests;
import org.elasticsearch.xpack.inference.services.custom.CustomSecretSettings;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;
import org.elasticsearch.xpack.inference.services.custom.CustomTaskSettings;
import org.elasticsearch.xpack.inference.services.custom.QueryParameters;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CustomRequestTests extends ESTestCase {

    public void testCreateRequest() throws IOException {
        var inferenceId = "inference_id";
        var dims = 1536;
        var maxInputTokens = 512;
        Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, Strings.format("${api_key}"));
        var requestContentString = """
            {
                "input": ${input}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            new CustomServiceSettings.TextEmbeddingSettings(
                SimilarityMeasure.DOT_PRODUCT,
                dims,
                maxInputTokens,
                DenseVectorFieldMapper.ElementType.FLOAT
            ),
            "${url}",
            headers,
            new QueryParameters(List.of(new QueryParameters.Parameter("key", "value"), new QueryParameters.Parameter("key", "value2"))),
            requestContentString,
            new TextEmbeddingResponseParser("$.result.embeddings"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            serviceSettings,
            new CustomTaskSettings(Map.of("url", "https://www.elastic.com")),
            new CustomSecretSettings(Map.of("api_key", new SerializableSecureString("my-secret-key")))
        );

        var request = new CustomRequest(null, List.of("abc", "123"), model);
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is("https://www.elastic.com?key=value&key=value2"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("my-secret-key"));

        var expectedBody = XContentHelper.stripWhitespace("""
            {
              "input": ["abc", "123"]
            }
            """);

        assertThat(convertToString(httpPost.getEntity().getContent()), is(expectedBody));
    }

    public void testCreateRequest_QueryParametersAreEscaped_AndEncoded() {
        var inferenceId = "inferenceId";
        var requestContentString = """
            {
                "input": ${input}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS,
            "http://www.elastic.co",
            null,
            // escaped characters retrieved from here: https://docs.microfocus.com/OMi/10.62/Content/OMi/ExtGuide/ExtApps/URL_encoding.htm
            new QueryParameters(
                List.of(
                    new QueryParameters.Parameter("key", " <>#%+{}|\\^~[]`;/?:@=&$"),
                    // unicode is a 😀
                    // Note: In the current version of the apache library (4.x) being used to do the encoding, spaces are converted to +
                    // There's a bug fix here explaining that: https://issues.apache.org/jira/browse/HTTPCORE-628
                    new QueryParameters.Parameter("key", "Σ \uD83D\uDE00")
                )
            ),
            requestContentString,
            new TextEmbeddingResponseParser("$.result.embeddings"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            serviceSettings,
            new CustomTaskSettings(Map.of("url", "https://www.elastic.com")),
            new CustomSecretSettings(Map.of("api_key", new SerializableSecureString("my-secret-key")))
        );

        var request = new CustomRequest(null, List.of("abc", "123"), model);
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(
            httpPost.getURI().toString(),
            // To visually verify that this is correct, input the query parameters into here: https://www.urldecoder.org/
            is("http://www.elastic.co?key=+%3C%3E%23%25%2B%7B%7D%7C%5C%5E%7E%5B%5D%60%3B%2F%3F%3A%40%3D%26%24&key=%CE%A3+%F0%9F%98%80")
        );
    }

    public void testCreateRequest_SecretsInTheJsonBody_AreEncodedCorrectly() throws IOException {
        var inferenceId = "inference_id";
        var dims = 1536;
        var maxInputTokens = 512;
        Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, Strings.format("${api_key}"));
        var requestContentString = """
            {
                "input": ${input},
                "secret": ${api_key}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            new CustomServiceSettings.TextEmbeddingSettings(
                SimilarityMeasure.DOT_PRODUCT,
                dims,
                maxInputTokens,
                DenseVectorFieldMapper.ElementType.FLOAT
            ),
            "${url}",
            headers,
            new QueryParameters(List.of(new QueryParameters.Parameter("key", "value"), new QueryParameters.Parameter("key", "value2"))),
            requestContentString,
            new TextEmbeddingResponseParser("$.result.embeddings"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            serviceSettings,
            new CustomTaskSettings(Map.of("url", "https://www.elastic.com")),
            new CustomSecretSettings(Map.of("api_key", new SerializableSecureString("my-secret-key")))
        );

        var request = new CustomRequest(null, List.of("abc", "123"), model);
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is("https://www.elastic.com?key=value&key=value2"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("my-secret-key"));

        // secret is encoded in json format (with quotes)
        var expectedBody = XContentHelper.stripWhitespace("""
            {
              "input": ["abc", "123"],
              "secret": "my-secret-key"
            }
            """);

        assertThat(convertToString(httpPost.getEntity().getContent()), is(expectedBody));
    }

    public void testCreateRequest_HandlesQuery() throws IOException {
        var inferenceId = "inference_id";
        var requestContentString = """
            {
                "input": ${input},
                "query": ${query}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS,
            "http://www.elastic.co",
            null,
            null,
            requestContentString,
            new RerankResponseParser("$.result.score"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.RERANK,
            serviceSettings,
            new CustomTaskSettings(Map.of()),
            new CustomSecretSettings(Map.of("api_key", new SerializableSecureString("my-secret-key")))
        );

        var request = new CustomRequest("query string", List.of("abc", "123"), model);
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var expectedBody = XContentHelper.stripWhitespace("""
            {
              "input": ["abc", "123"],
              "query": "query string"
            }
            """);

        assertThat(convertToString(httpPost.getEntity().getContent()), is(expectedBody));
    }

    public void testCreateRequest_IgnoresNonStringFields_ForStringParams() throws IOException {
        var inferenceId = "inference_id";
        var requestContentString = """
            {
                "input": ${input}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS,
            "http://www.elastic.co",
            Map.of(HttpHeaders.ACCEPT, Strings.format("${task.key}")),
            null,
            requestContentString,
            new RerankResponseParser("$.result.score"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.RERANK,
            serviceSettings,
            new CustomTaskSettings(Map.of("task.key", 100)),
            new CustomSecretSettings(Map.of("api_key", new SerializableSecureString("my-secret-key")))
        );

        var request = new CustomRequest(null, List.of("abc", "123"), model);
        var exception = expectThrows(IllegalStateException.class, request::createHttpRequest);
        assertThat(exception.getMessage(), is("Found placeholder [${task.key}] in field [header.Accept] after replacement call"));
    }

    public void testCreateRequest_ThrowsException_ForInvalidUrl() {
        var inferenceId = "inference_id";
        var requestContentString = """
            {
                "input": ${input}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            CustomServiceSettings.TextEmbeddingSettings.NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS,
            "${url}",
            Map.of(HttpHeaders.ACCEPT, Strings.format("${task.key}")),
            null,
            requestContentString,
            new RerankResponseParser("$.result.score"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        var model = CustomModelTests.createModel(
            inferenceId,
            TaskType.RERANK,
            serviceSettings,
            new CustomTaskSettings(Map.of("url", "^")),
            new CustomSecretSettings(Map.of("api_key", new SerializableSecureString("my-secret-key")))
        );

        var exception = expectThrows(IllegalStateException.class, () -> new CustomRequest(null, List.of("abc", "123"), model));
        assertThat(exception.getMessage(), is("Failed to build URI, error: Illegal character in path at index 0: ^"));
    }

    private static String convertToString(InputStream inputStream) throws IOException {
        return XContentHelper.stripWhitespace(Streams.copyToString(new InputStreamReader(inputStream, StandardCharsets.UTF_8)));
    }
}
