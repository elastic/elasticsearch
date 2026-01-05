/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModelTests;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxEmbeddingsRequestTests extends ESTestCase {
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
        var input = "input";

        var request = createRequest(model, projectId, uri, apiVersion, apiKey, input, null, null);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "version", apiVersion)));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(
            requestMap,
            is(

                Map.of("project_id", "project_id", "model_id", "model", "inputs", List.of(input))

            )
        );
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var model = "model";
        var projectId = "project_id";
        URI uri = null;
        try {
            uri = new URI("http://abc.com");
        } catch (Exception ignored) {}
        var apiVersion = "2023-05-04";
        var apiKey = "api_key";
        var input = "abcd";

        var request = createRequest(model, projectId, uri, apiVersion, apiKey, input, null, null);
        var truncatedRequest = request.truncate();
        var httpRequest = truncatedRequest.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "version", apiVersion)));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(
            requestMap,
            is(

                Map.of("project_id", "project_id", "model_id", "model", "inputs", List.of("ab"))

            )
        );
    }

    public void testIsTruncated_ReturnsTrue() {
        var model = "model";
        var projectId = "project_id";
        URI uri = null;
        try {
            uri = new URI("http://abc.com");
        } catch (Exception ignored) {}
        var apiVersion = "2023-05-04";
        var apiKey = "api_key";
        var input = "abcd";

        var request = createRequest(model, projectId, uri, apiVersion, apiKey, input, null, null);
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    public static IbmWatsonxEmbeddingsRequest createRequest(
        String model,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        String input,
        @Nullable Integer maxTokens,
        @Nullable Integer dimensions
    ) {
        var embeddingsModel = IbmWatsonxEmbeddingsModelTests.createModel(model, projectId, uri, apiVersion, apiKey, maxTokens, dimensions);

        return new IbmWatsonxEmbeddingsWithoutAuthRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            embeddingsModel
        );
    }

    private static class IbmWatsonxEmbeddingsWithoutAuthRequest extends IbmWatsonxEmbeddingsRequest {
        IbmWatsonxEmbeddingsWithoutAuthRequest(Truncator truncator, Truncator.TruncationResult input, IbmWatsonxEmbeddingsModel model) {
            super(truncator, input, model);
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }

        @Override
        public Request truncate() {
            IbmWatsonxEmbeddingsRequest embeddingsRequest = (IbmWatsonxEmbeddingsRequest) super.truncate();
            return new IbmWatsonxEmbeddingsWithoutAuthRequest(
                embeddingsRequest.truncator(),
                embeddingsRequest.truncationResult(),
                embeddingsRequest.model()
            );
        }
    }
}
