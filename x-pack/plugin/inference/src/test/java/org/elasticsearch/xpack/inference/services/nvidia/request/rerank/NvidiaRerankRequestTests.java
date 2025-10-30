/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.rerank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModelTests;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NvidiaRerankRequestTests extends ESTestCase {
    private static final String PASSAGES = "passages";
    private static final String QUERY = "query";
    private static final String MODEL_ID = "model";

    private static final String AUTH_HEADER_VALUE = "Bearer secret";

    public void testCreateRequest_AllFieldsSet() throws IOException {
        testCreateRequest(createRequest(MODEL_ID, "url"), "url");
    }

    public void testCreateRequest_NoUrl() throws IOException {
        testCreateRequest(createRequest(MODEL_ID, null), "https://ai.api.nvidia.com/v1/retrieval/nvidia/reranking");
    }

    public void testCreateRequest_NoModel_ThrowsException() {
        expectThrows(NullPointerException.class, () -> createRequest(null, "url"));
    }

    private void testCreateRequest(NvidiaRerankRequest request, String expectedUrl)
        throws IOException {
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), Matchers.is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(PASSAGES), is(List.of(Map.of("text", PASSAGES))));
        assertThat(requestMap.get(QUERY), is(Map.of("text",QUERY)));
            assertThat(requestMap.get("model"), is(MODEL_ID));
        assertThat(requestMap, aMapWithSize(3));
    }

    private static NvidiaRerankRequest createRequest(
        @Nullable String modelId, @Nullable String url
    ) {
        var rerankModel = NvidiaRerankModelTests.createModel(url, "secret", modelId);
        return new NvidiaRerankRequest(QUERY, List.of(PASSAGES), rerankModel);
    }
}
