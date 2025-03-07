/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_UrlDefined() throws IOException {
        var request = createRequest(
            List.of("abc"),
            CohereEmbeddingsModelTests.createModel("url", "secret", CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, null, null)
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap, is(Map.of("texts", List.of("abc"), "embedding_types", List.of("float"))));
    }

    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var request = createRequest(
            List.of("abc"),
            CohereEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START),
                null,
                null,
                "model",
                CohereEmbeddingType.FLOAT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(
            requestMap,
            is(
                Map.of(
                    "texts",
                    List.of("abc"),
                    "model",
                    "model",
                    "input_type",
                    "search_document",
                    "embedding_types",
                    List.of("float"),
                    "truncate",
                    "start"
                )
            )
        );
    }

    public void testCreateRequest_InputTypeSearch_EmbeddingTypeInt8_TruncateEnd() throws IOException {
        var request = createRequest(
            List.of("abc"),
            CohereEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.END),
                null,
                null,
                "model",
                CohereEmbeddingType.INT8
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(
            requestMap,
            is(
                Map.of(
                    "texts",
                    List.of("abc"),
                    "model",
                    "model",
                    "input_type",
                    "search_query",
                    "embedding_types",
                    List.of("int8"),
                    "truncate",
                    "end"
                )
            )
        );
    }

    public void testCreateRequest_InputTypeSearch_EmbeddingTypeBit_TruncateEnd() throws IOException {
        var request = createRequest(
            List.of("abc"),
            CohereEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.END),
                null,
                null,
                "model",
                CohereEmbeddingType.BIT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(
            requestMap,
            is(
                Map.of(
                    "texts",
                    List.of("abc"),
                    "model",
                    "model",
                    "input_type",
                    "search_query",
                    "embedding_types",
                    List.of("binary"),
                    "truncate",
                    "end"
                )
            )
        );
    }

    public void testCreateRequest_TruncateNone() throws IOException {
        var request = createRequest(
            List.of("abc"),
            CohereEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE),
                null,
                null,
                null,
                null
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(CohereUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap, is(Map.of("texts", List.of("abc"), "truncate", "none", "embedding_types", List.of("float"))));
    }

    public static CohereEmbeddingsRequest createRequest(List<String> input, CohereEmbeddingsModel model) {
        return new CohereEmbeddingsRequest(input, model);
    }
}
