/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudMultimodalEmbeddingsTaskSettings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class TencentCloudMultimodalEmbeddingsRequestTests extends ESTestCase {

    public void testCreateHttpRequest() throws Exception {
        var model = TencentCloudEmbeddingsModelTests.createEmbeddingModel(TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS);
        var inputs = List.of(
            new InferenceString(DataType.TEXT, "hello"),
            new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/a.png")
        );
        var request = new TencentCloudMultimodalEmbeddingsRequest(inputs, null, model);

        var httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = httpRequest.httpRequest();
        assertThat(httpPost.getMethod(), is("POST"));

        assertThat(httpPost.getUri().toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/multimodal-embeddings"));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", "sk-test")));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), containsString("application/json"));
        assertThat(
            entityAsMap(httpPost.getBodyText()),
            is(Map.of("model", "WeMM-Embedding-2B", "texts", List.of("hello"), "image_url", List.of("https://example.com/a.png")))
        );
        assertThat(request.getInputs(), is(inputs));
        assertThat(request.getInferenceEntityId(), is("test-inference-id"));
    }
}
