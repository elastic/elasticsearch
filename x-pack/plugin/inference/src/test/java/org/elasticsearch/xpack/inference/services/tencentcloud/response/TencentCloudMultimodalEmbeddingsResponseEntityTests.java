/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.response;

import org.apache.hc.core5.http.HttpResponse;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudMultimodalEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudMultimodalEmbeddingsRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TencentCloudMultimodalEmbeddingsResponseEntityTests extends ESTestCase {

    private static final InferenceString IMAGE = new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/a.png");
    private static final InferenceString VIDEO = new InferenceString(DataType.VIDEO, DataFormat.BASE64, "data:video/mp4;base64,abcd");

    public void testFromResponse_ReturnsEmbeddingsInInputOrder() throws IOException {
        var inputs = List.of(text("t0"), IMAGE, text("t1"), VIDEO);
        var responseJson = """
            {
              "object": "multimodal_embedding",
              "data": {
                "video_embeddings": [ { "object": "embedding", "embedding": [0.5, 0.6], "index": 0 } ],
                "text_embeddings": [
                  { "object": "embedding", "embedding": [0.3, 0.4], "index": 1 },
                  { "object": "embedding", "embedding": [0.1, 0.2], "index": 0 }
                ],
                "image_embeddings": [ { "object": "embedding", "embedding": [0.7, 0.8], "index": 0 } ]
              },
              "model": "WeMM-Embedding-2B",
              "usage": { "total_tokens": 10, "prompt_tokens_details": { "image_tokens": 4 } }
            }
            """;

        var results = TencentCloudMultimodalEmbeddingsResponseEntity.fromResponse(request(inputs), httpResult(responseJson));

        assertThat(
            results.embeddings(),
            is(
                List.of(
                    EmbeddingFloatResults.Embedding.of(List.of(0.1F, 0.2F)),
                    EmbeddingFloatResults.Embedding.of(List.of(0.7F, 0.8F)),
                    EmbeddingFloatResults.Embedding.of(List.of(0.3F, 0.4F)),
                    EmbeddingFloatResults.Embedding.of(List.of(0.5F, 0.6F))
                )
            )
        );
    }

    public void testFromResponse_MissingEmbedding_Throws() {
        var responseJson = """
            {
              "data": { "text_embeddings": [ { "embedding": [0.1], "index": 0 } ] }
            }
            """;

        var exception = expectThrows(
            IllegalStateException.class,
            () -> TencentCloudMultimodalEmbeddingsResponseEntity.fromResponse(request(List.of(text("t0"), IMAGE)), httpResult(responseJson))
        );
        assertThat(exception.getMessage(), containsString("missing the [image] embedding at index [0]"));
    }

    public void testFromResponse_ExtraEmbedding_Throws() {
        var responseJson = """
            {
              "data": {
                "text_embeddings": [ { "embedding": [0.1], "index": 0 }, { "embedding": [0.2], "index": 1 } ]
              }
            }
            """;

        var exception = expectThrows(
            IllegalStateException.class,
            () -> TencentCloudMultimodalEmbeddingsResponseEntity.fromResponse(request(List.of(text("t0"))), httpResult(responseJson))
        );
        assertThat(exception.getMessage(), containsString("contains [2] [text] embeddings but [1] were requested"));
    }

    public void testFromResponse_DuplicateIndex_Throws() {
        var responseJson = """
            {
              "data": {
                "text_embeddings": [ { "embedding": [0.1], "index": 0 }, { "embedding": [0.2], "index": 0 } ]
              }
            }
            """;

        var exception = expectThrows(
            IllegalStateException.class,
            () -> TencentCloudMultimodalEmbeddingsResponseEntity.fromResponse(
                request(List.of(text("t0"), text("t1"))),
                httpResult(responseJson)
            )
        );
        assertThat(exception.getMessage(), containsString("invalid index [0]"));
    }

    public void testFromResponse_MissingData_Throws() {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> TencentCloudMultimodalEmbeddingsResponseEntity.fromResponse(
                request(List.of(text("t0"))),
                httpResult("{\"model\": \"m\"}")
            )
        );
        assertThat(exception.getMessage(), containsString("Failed to find required field [data]"));
    }

    private static InferenceString text(String value) {
        return new InferenceString(DataType.TEXT, value);
    }

    private static TencentCloudMultimodalEmbeddingsRequest request(List<InferenceString> inputs) {
        var model = TencentCloudEmbeddingsModelTests.createEmbeddingModel(TencentCloudMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS);
        return new TencentCloudMultimodalEmbeddingsRequest(inputs, null, model);
    }

    private static HttpResult httpResult(String body) {
        return new HttpResult(mock(HttpResponse.class), body.getBytes(StandardCharsets.UTF_8));
    }
}
