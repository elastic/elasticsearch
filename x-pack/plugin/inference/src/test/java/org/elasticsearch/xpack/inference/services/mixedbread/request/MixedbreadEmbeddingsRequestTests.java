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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.common.model.Truncation;
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
    private static final InputType INPUT_TYPE_ELASTIC_INITIAL_VALUE = InputType.INGEST;
    private static final Truncation TRUNCATE_ELASTIC_VALUE = Truncation.START;

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        var request = createRequest(TestUtils.CUSTOM_URL, INPUT_TYPE_ELASTIC_INITIAL_VALUE, TRUNCATE_ELASTIC_VALUE, null);
        var requestMap = getEntityAsMap(request);
        assertThat(requestMap, aMapWithSize(4));
        assertThat(requestMap.get("input"), is(List.of(INPUT)));
        assertThat(requestMap.get("truncate"), is("start"));
        assertThat(requestMap.get("input_type"), is("passage"));
        assertThat(requestMap.get("model"), is(TestUtils.MODEL_ID));
    }

    public void testGetTruncationInfo() {
        var request = createRequest(TestUtils.CUSTOM_URL, null, null, null);
        assertThat(request.getTruncationInfo()[0], is(false));

        var truncatedRequest = request.truncate();
        assertThat(truncatedRequest.getTruncationInfo()[0], is(true));
    }

    private static MixedbreadEmbeddingsRequest createRequest(
        @Nullable String url,
        @Nullable InputType inputType,
        @Nullable Truncation truncation,
        @Nullable InputType requestInputType
    ) {
        var embeddingsModel = MixedbreadEmbeddingsModelTests.createModel(
            url,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            null,
            null,
            inputType,
            truncation,
            null
        );
        return new MixedbreadEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(INPUT), new boolean[] { false }),
            embeddingsModel,
            requestInputType
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
