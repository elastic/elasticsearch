/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OciGenAiRerankResponseEntityTests extends ESTestCase {

    public void testFromResponse_WithDocuments() throws IOException {
        String responseJson = """
            {
                "documentRanks": [
                    { "document": "Paris is the capital of France.", "index": 0, "relevanceScore": 0.98 },
                    { "document": "Berlin is in Germany.", "index": 2, "relevanceScore": 0.12 }
                ],
                "id": "abc",
                "modelId": "cohere.rerank-v3.5",
                "modelVersion": "3.5"
            }
            """;

        var results = OciGenAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            results.getRankedDocs(),
            is(
                List.of(
                    new RankedDocsResults.RankedDoc(0, 0.98F, "Paris is the capital of France."),
                    new RankedDocsResults.RankedDoc(2, 0.12F, "Berlin is in Germany.")
                )
            )
        );
    }

    public void testFromResponse_WithoutDocuments() throws IOException {
        String responseJson = """
            { "documentRanks": [ { "index": 1, "relevanceScore": 0.5 } ], "modelId": "cohere.rerank-v3.5" }
            """;

        var results = OciGenAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(results.getRankedDocs(), is(List.of(new RankedDocsResults.RankedDoc(1, 0.5F, null))));
    }

    public void testFromResponse_FailsWithoutDocumentRanks() {
        String responseJson = """
            { "modelId": "cohere.rerank-v3.5" }
            """;

        expectThrows(
            Exception.class,
            () -> OciGenAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
    }
}
