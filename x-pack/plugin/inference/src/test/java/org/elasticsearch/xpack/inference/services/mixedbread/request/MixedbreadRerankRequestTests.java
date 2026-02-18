/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.mixedbread.request.rerank.MixedbreadRerankRequest;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadServiceTests.RETURN_DOCUMENTS_FALSE;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadRerankRequestTests extends ESTestCase {

    private static final String API_KEY = "secret";
    public static final String INPUT = "input_value";
    public static final String MODEL = "model_id_value";
    public static final String QUERY = "query_value";
    public static final int TOP_K = 1;

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        var request = createRequest(QUERY, INPUT, MODEL, null, null);
        var requestMap = getEntityAsMap(request);
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("input"), is(List.of(INPUT)));
        assertThat(requestMap.get("query"), is(QUERY));
        assertThat(requestMap.get("model"), is(MODEL));
    }

    public void testCreateRequest_WithAllFieldsSet() throws IOException {
        var request = createRequest(QUERY, INPUT, MODEL, TOP_K, RETURN_DOCUMENTS_FALSE);
        Map<String, Object> requestMap = getEntityAsMap(request);
        assertThat(requestMap, aMapWithSize(5));
        assertThat(requestMap.get("input"), is(List.of(INPUT)));
        assertThat(requestMap.get("query"), is(QUERY));
        assertThat(requestMap.get("top_k"), is(TOP_K));
        assertThat(requestMap.get("return_input"), is(RETURN_DOCUMENTS_FALSE));
        assertThat(requestMap.get("model"), is(MODEL));
    }

    public void testTruncate_DoesNotTruncate() {
        var request = createRequest(QUERY, INPUT, "null", null, null);
        var truncatedRequest = request.truncate();

        assertThat(truncatedRequest, sameInstance(request));
    }

    private static MixedbreadRerankRequest createRequest(
        String query,
        String input,
        @Nullable String modelId,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments
    ) {
        var rerankModel = MixedbreadRerankModelTests.createModel(modelId, API_KEY, null, null, null);
        return new MixedbreadRerankRequest(query, List.of(input), returnDocuments, topN, rerankModel);
    }

    private Map<String, Object> getEntityAsMap(MixedbreadRerankRequest request) throws IOException {
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + API_KEY));
        assertThat(httpPost.getURI(), is(sameInstance(request.getURI())));
        return entityAsMap(httpPost.getEntity().getContent());
    }
}
