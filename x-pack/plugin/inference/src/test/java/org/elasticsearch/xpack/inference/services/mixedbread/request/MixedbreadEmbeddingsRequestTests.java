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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.mixedbread.request.embeddings.MixedbreadEmbeddingsRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadEmbeddingsRequestTests extends ESTestCase {
    public static final String INPUT = "input_value";
    public static final String QUERY = "query_value";

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        var request = createRequest(TestUtils.CUSTOM_URL, null, null);
        var requestMap = getEntityAsMap(request);
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("input"), is(List.of(INPUT)));
        assertThat(requestMap.get("model"), is(TestUtils.MODEL_ID));
        assertThat(requestMap.get("encoding_format"), is(TestUtils.ENCODING_VALUE));
    }

    public void testCreateRequest_WithAllFieldsSets() throws IOException {
        var request = createRequest(TestUtils.CUSTOM_URL, TestUtils.PROMPT_INITIAL_VALUE, TestUtils.NORMALIZED_INITIAL_VALUE);
        var requestMap = getEntityAsMap(request);
        assertThat(requestMap, aMapWithSize(5));
        assertThat(requestMap.get("input"), is(List.of(INPUT)));
        assertThat(requestMap.get("model"), is(TestUtils.MODEL_ID));
        assertThat(requestMap.get("prompt"), is(TestUtils.PROMPT_INITIAL_VALUE));
        assertThat(requestMap.get("normalized"), is(TestUtils.NORMALIZED));
        assertThat(requestMap.get("encoding_format"), is(TestUtils.ENCODING_VALUE));
    }

    public void testGetTruncationInfo() {
        var request = createRequest(TestUtils.CUSTOM_URL, null, null);
        assertThat(request.getTruncationInfo()[0], is(false));

        var truncatedRequest = request.truncate();
        assertThat(truncatedRequest.getTruncationInfo()[0], is(true));
    }

    private static MixedbreadEmbeddingsRequest createRequest(@Nullable String url, @Nullable String prompt, @Nullable Boolean normalized) {
        var embeddingsModel = MixedbreadEmbeddingsModelTests.createModel(
            url,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            null,
            null,
            prompt,
            normalized,
            null
        );
        return new MixedbreadEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(INPUT), new boolean[] { false }),
            embeddingsModel
        );
    }

    private Map<String, Object> getEntityAsMap(MixedbreadEmbeddingsRequest request) throws IOException {
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + TestUtils.API_KEY));
        assertThat(httpPost.getURI(), is(sameInstance(request.getURI())));
        return entityAsMap(httpPost.getEntity().getContent());
    }
}
