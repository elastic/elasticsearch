/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModelTests;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxRerankRequestTests extends ESTestCase {
    private static final String AUTH_HEADER_VALUE = "foo";

    public void testCreateRequest() throws IOException {
        var model = "model";
        var projectId = "project_id";
        URI uri = null;
        try {
            uri = new URI("http://abc.com");
        } catch (Exception ignored) {}
        var apiVersion = "2023-05-04";
        var apiKey = "api_key";
        var query = "database";
        List<String> input = List.of("greenland", "google", "john", "mysql", "potter", "grammar");

        var request = createRequest(model, projectId, uri, apiVersion, apiKey, query, input);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "version", apiVersion)));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(5));
        assertThat(
            requestMap,
            is(

                Map.of(
                    "project_id",
                    "project_id",
                    "model_id",
                    "model",
                    "inputs",
                    List.of(
                        Map.of("text", "greenland"),
                        Map.of("text", "google"),
                        Map.of("text", "john"),
                        Map.of("text", "mysql"),
                        Map.of("text", "potter"),
                        Map.of("text", "grammar")
                    ),
                    "query",
                    "database",
                    "parameters",
                    Map.of("return_options", Map.of("top_n", 2, "inputs", true), "truncate_input_tokens", 100)
                )
            )
        );
    }

    public static IbmWatsonxRerankRequest createRequest(
        String model,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        String query,
        List<String> input
    ) {
        var embeddingsModel = IbmWatsonxRerankModelTests.createModel(model, projectId, uri, apiVersion, apiKey);

        return new IbmWatsonxRerankWithoutAuthRequest(query, input, embeddingsModel);
    }

    private static class IbmWatsonxRerankWithoutAuthRequest extends IbmWatsonxRerankRequest {
        IbmWatsonxRerankWithoutAuthRequest(String query, List<String> input, IbmWatsonxRerankModel model) {
            super(query, input, model);
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }
    }
}
