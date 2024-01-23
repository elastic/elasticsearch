/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_UrlDefined() throws URISyntaxException, IOException {
        var request = createRequest("url", "secret", List.of("abc"), CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null);

        var httpRequest = request.createRequest();
        MatcherAssert.assertThat(httpRequest, instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest;

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap, is(Map.of("texts", List.of("abc"))));
    }

    public void testCreateRequest_AllOptionsDefined() throws URISyntaxException, IOException {
        var request = createRequest(
            "url",
            "secret",
            List.of("abc"),
            new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START),
            "model",
            CohereEmbeddingType.FLOAT
        );

        var httpRequest = request.createRequest();
        MatcherAssert.assertThat(httpRequest, instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest;

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

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

    public void testCreateRequest_InputTypeSearch_EmbeddingTypeInt8_TruncateEnd() throws URISyntaxException, IOException {
        var request = createRequest(
            "url",
            "secret",
            List.of("abc"),
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.END),
            "model",
            CohereEmbeddingType.INT8
        );

        var httpRequest = request.createRequest();
        MatcherAssert.assertThat(httpRequest, instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest;

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

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

    public void testCreateRequest_TruncateNone() throws URISyntaxException, IOException {
        var request = createRequest(
            "url",
            "secret",
            List.of("abc"),
            new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE),
            null,
            null
        );

        var httpRequest = request.createRequest();
        MatcherAssert.assertThat(httpRequest, instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest;

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestMap, is(Map.of("texts", List.of("abc"), "truncate", "none")));
    }

    public static CohereEmbeddingsRequest createRequest(
        @Nullable String url,
        String apiKey,
        List<String> input,
        CohereEmbeddingsTaskSettings taskSettings,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType
    ) throws URISyntaxException {
        var uri = url == null ? null : new URI(url);

        var account = new CohereAccount(uri, new SecureString(apiKey.toCharArray()));
        return new CohereEmbeddingsRequest(account, input, taskSettings, model, embeddingType);
    }
}
