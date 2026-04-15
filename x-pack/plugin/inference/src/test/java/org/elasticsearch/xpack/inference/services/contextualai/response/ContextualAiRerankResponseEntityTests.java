/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ContextualAiRerankResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "index": 0,
                        "relevance_score": 0.95
                    }
                ]
            }
            """;

        RankedDocsResults parsedResults = ContextualAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.getRankedDocs(), is(List.of(new RankedDocsResults.RankedDoc(0, 0.95F, null))));
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "index": 0,
                        "relevance_score": 0.94
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.78
                    },
                    {
                        "index": 2,
                        "relevance_score": 0.65
                    }
                ]
            }
            """;

        RankedDocsResults parsedResults = ContextualAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.getRankedDocs(),
            is(
                List.of(
                    new RankedDocsResults.RankedDoc(0, 0.94F, null),
                    new RankedDocsResults.RankedDoc(1, 0.78F, null),
                    new RankedDocsResults.RankedDoc(2, 0.65F, null)
                )
            )
        );
    }

    public void testFromResponse_HandlesEmptyResultsList() throws IOException {
        String responseJson = """
            {
                "results": []
            }
            """;

        RankedDocsResults parsedResults = ContextualAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.getRankedDocs(), is(List.of()));
    }

    public void testFromResponse_HandlesFloatingPointPrecision() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "index": 0,
                        "relevance_score": 0.9432156
                    }
                ]
            }
            """;

        RankedDocsResults parsedResults = ContextualAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.getRankedDocs(), is(List.of(new RankedDocsResults.RankedDoc(0, 0.9432156F, null))));
    }

    public void testFromResponse_OrderIsPreserved() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "index": 2,
                        "relevance_score": 0.94
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.78
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.65
                    }
                ]
            }
            """;

        RankedDocsResults parsedResults = ContextualAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.getRankedDocs(),
            is(
                List.of(
                    new RankedDocsResults.RankedDoc(2, 0.94F, null),
                    new RankedDocsResults.RankedDoc(0, 0.78F, null),
                    new RankedDocsResults.RankedDoc(1, 0.65F, null)
                )
            )
        );
    }

    public void testFromResponse_TextIsAlwaysNull() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "index": 0,
                        "relevance_score": 0.9
                    }
                ]
            }
            """;

        RankedDocsResults parsedResults = ContextualAiRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertNull(parsedResults.getRankedDocs().getFirst().text());
    }

    public void testFromResponse_FailsWhenResultsFieldIsNotPresent() {
        String responseJson = """
            {
                "not_results": [
                    {
                        "index": 0,
                        "relevance_score": 0.95
                    }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> ContextualAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("Required [results]"));
    }

    public void testFromResponse_FailsWhenIndexFieldIsNotPresent() {
        String responseJson = """
            {
                "results": [
                    {
                        "relevance_score": 0.95
                    }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> ContextualAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("[contextualai_rerank_response] failed to parse field [results]"));
    }

    public void testFromResponse_FailsWhenRelevanceScoreFieldIsNotPresent() {
        String responseJson = """
            {
                "results": [
                    {
                        "index": 0
                    }
                ]
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> ContextualAiRerankResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("[contextualai_rerank_response] failed to parse field [results]"));
    }
}
