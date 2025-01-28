/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.googlevertexai;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiRerankResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                 "records": [
                     {
                         "id": "2",
                         "title": "title 2",
                         "content": "content 2",
                         "score": 0.97
                     }
                ]
            }
            """;

        RankedDocsResults parsedResults = GoogleVertexAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.getRankedDocs(), is(List.of(new RankedDocsResults.RankedDoc(2, 0.97F, "content 2"))));
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                 "records": [
                     {
                         "id": "2",
                         "title": "title 2",
                         "content": "content 2",
                         "score": 0.97
                     },
                     {
                         "id": "1",
                         "title": "title 1",
                         "content": "content 1",
                         "score": 0.90
                     }
                ]
            }
            """;

        RankedDocsResults parsedResults = GoogleVertexAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.getRankedDocs(),
            is(List.of(new RankedDocsResults.RankedDoc(2, 0.97F, "content 2"), new RankedDocsResults.RankedDoc(1, 0.90F, "content 1")))
        );
    }

    public void testFromResponse_FailsWhenRecordsFieldIsNotPresent() {
        String responseJson = """
            {
                 "not_records": [
                     {
                         "id": "2",
                         "title": "title 2",
                         "content": "content 2",
                         "score": 0.97
                     },
                     {
                         "id": "1",
                         "title": "title 1",
                         "content": "content 1",
                         "score": 0.90
                     }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [records] in Google Vertex AI rerank response"));
    }

    public void testFromResponse_FailsWhenContentFieldIsNotPresent() {
        String responseJson = """
            {
                 "records": [
                     {
                         "id": "2",
                         "title": "title 2",
                         "content": "content 2",
                         "score": 0.97
                     },
                     {
                        "id": "1",
                        "title": "title 1",
                        "not_content": "content 1",
                        "score": 0.97
                     }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [content] in Google Vertex AI rerank response"));
    }

    public void testFromResponse_FailsWhenScoreFieldIsNotPresent() {
        String responseJson = """
            {
                 "records": [
                     {
                         "id": "2",
                         "title": "title 2",
                         "content": "content 2",
                         "not_score": 0.97
                     },
                     {
                        "id": "1",
                        "title": "title 1",
                        "content": "content 1",
                        "score": 0.96
                     }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [score] in Google Vertex AI rerank response"));
    }

    public void testFromResponse_FailsWhenIDFieldIsNotInteger() {
        String responseJson = """
            {
                 "records": [
                     {
                         "id": "abcd",
                         "title": "title 2",
                         "content": "content 2",
                         "score": 0.97
                     },
                     {
                        "id": "1",
                        "title": "title 1",
                        "content": "content 1",
                        "score": 0.96
                     }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> GoogleVertexAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Expected numeric value for record ID field in Google Vertex AI rerank response but received [abcd]")
        );
    }
}
