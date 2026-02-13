/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.rerank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.MODEL_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.PASSAGES_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.QUERY_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.TEXT_FIELD_NAME;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NvidiaRerankRequestTests extends ESTestCase {

    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_DEFAULT_VALUE = "https://ai.api.nvidia.com/v1/retrieval/nvidia/reranking";
    private static final String PASSAGE_VALUE = "some document";
    private static final String QUERY_VALUE = "some query";
    private static final String MODEL_VALUE = "some_model";
    private static final String API_KEY_VALUE = "test_api_key";

    public void testCreateRequest_AllFieldsSet() throws IOException {
        assertCreateHttpRequest(createRequest(URL_VALUE), URL_VALUE);
    }

    public void testCreateRequest_DefaultUrl() throws IOException {
        assertCreateHttpRequest(createRequest(null), URL_DEFAULT_VALUE);
    }

    private void assertCreateHttpRequest(NvidiaRerankRequest request, String expectedUrl) throws IOException {
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(PASSAGES_FIELD_NAME), is(List.of(Map.of(TEXT_FIELD_NAME, PASSAGE_VALUE))));
        assertThat(requestMap.get(QUERY_FIELD_NAME), is(Map.of(TEXT_FIELD_NAME, QUERY_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        assertThat(requestMap, aMapWithSize(3));
    }

    private static NvidiaRerankRequest createRequest(@Nullable String url) {
        var rerankModel = NvidiaRerankModelTests.createRerankModel(url, API_KEY_VALUE, MODEL_VALUE);
        return new NvidiaRerankRequest(QUERY_VALUE, List.of(PASSAGE_VALUE), rerankModel);
    }
}
