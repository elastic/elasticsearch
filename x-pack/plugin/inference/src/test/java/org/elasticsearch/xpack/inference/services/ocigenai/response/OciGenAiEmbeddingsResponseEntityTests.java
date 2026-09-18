/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OciGenAiEmbeddingsResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                "embeddings": [
                    [ -0.018459704, 0.01399175 ],
                    [ 0.030681048, 0.01714732 ]
                ],
                "id": "1b6c1d1c-1e44-4a3a-9d2e-1234567890ab",
                "modelId": "cohere.embed-v4.0",
                "modelVersion": "4.0",
                "usage": { "completionTokens": 0, "promptTokens": 4, "totalTokens": 4 }
            }
            """;

        var parsedResults = OciGenAiEmbeddingsResponseEntity.fromResponse(
            mock(OutboundRequest.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    DenseEmbeddingFloatResults.Embedding.of(List.of(-0.018459704F, 0.01399175F)),
                    DenseEmbeddingFloatResults.Embedding.of(List.of(0.030681048F, 0.01714732F))
                )
            )
        );
    }

    public void testFromResponse_FailsWhenEmbeddingsFieldIsNotPresent() {
        String responseJson = """
            { "id": "abc", "modelId": "cohere.embed-v4.0" }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> OciGenAiEmbeddingsResponseEntity.fromResponse(
                mock(OutboundRequest.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [embeddings] in OCI Generative AI embeddings response"));
    }
}
