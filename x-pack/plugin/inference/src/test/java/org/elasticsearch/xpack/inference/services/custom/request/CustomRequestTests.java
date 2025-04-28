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
        var dims = 1536;
        var maxInputTokens = 512;
        Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, Strings.format("${api_key}"));
        var requestContentString = """
            {
                "input": ${input}
            }
            """;

        var serviceSettings = new CustomServiceSettings(
            SimilarityMeasure.DOT_PRODUCT,
            dims,
            maxInputTokens,
            "${url}",
            headers,
            new QueryParameters(List.of(new QueryParameters.Parameter("key", "value"), new QueryParameters.Parameter("key", "value2"))),
            requestContentString,
            new TextEmbeddingResponseParser("$.result.embeddings"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message")
        );

        var model = CustomModelTests.createModel(
            "service",
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

    public void testCreateRequest_SecretsInTheJsonBody_AreEncodedCorrectly() throws IOException {
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
            SimilarityMeasure.DOT_PRODUCT,
            dims,
            maxInputTokens,
            "${url}",
            headers,
            new QueryParameters(List.of(new QueryParameters.Parameter("key", "value"), new QueryParameters.Parameter("key", "value2"))),
            requestContentString,
            new TextEmbeddingResponseParser("$.result.embeddings"),
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message")
        );

        var model = CustomModelTests.createModel(
            "service",
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

    private static String convertToString(InputStream inputStream) throws IOException {
        return XContentHelper.stripWhitespace(Streams.copyToString(new InputStreamReader(inputStream, StandardCharsets.UTF_8)));
    }
}
