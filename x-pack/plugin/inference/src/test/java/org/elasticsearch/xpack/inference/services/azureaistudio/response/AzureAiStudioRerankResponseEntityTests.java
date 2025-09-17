/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureAiStudioRerankResponseEntityTests extends ESTestCase {
    public void testResponse_WithDocuments() throws IOException {
        final String responseJson = getResponseJsonWithDocuments();

        final var parsedResults = getParsedResults(responseJson);
        final var expectedResults = List.of(
            new RankedDocsResults.RankedDoc(0, 0.1111111F, "test text one"),
            new RankedDocsResults.RankedDoc(1, 0.2222222F, "test text two")
        );

        assertThat(parsedResults.getRankedDocs(), is(expectedResults));
    }

    public void testResponse_NoDocuments() throws IOException {
        final String responseJson = getResponseJsonNoDocuments();

        final var parsedResults = getParsedResults(responseJson);
        final var expectedResults = List.of(
            new RankedDocsResults.RankedDoc(0, 0.1111111F, null),
            new RankedDocsResults.RankedDoc(1, 0.2222222F, null)
        );

        assertThat(parsedResults.getRankedDocs(), is(expectedResults));
    }

    private RankedDocsResults getParsedResults(String responseJson) throws IOException {
        final var entity = new AzureAiStudioRerankResponseEntity();
        return (RankedDocsResults) entity.apply(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
    }

    private String getResponseJsonWithDocuments() {
        return """
            {
                "id": "222e59de-c712-40cb-ae87-ecd402d0d2f1",
                "results": [
                    {
                        "document": {
                            "text": "test text one"
                        },
                        "index": 0,
                        "relevance_score": 0.1111111
                    },
                    {
                        "document": {
                            "text": "test text two"
                        },
                        "index": 1,
                        "relevance_score": 0.2222222
                    }
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "search_units": 1
                    }
                }
            }
            """;
    }

    private String getResponseJsonNoDocuments() {
        return """
            {
                "id": "222e59de-c712-40cb-ae87-ecd402d0d2f1",
                "results": [
                    {
                        "index": 0,
                        "relevance_score": 0.1111111
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.2222222
                    }
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "search_units": 1
                    }
                }
            }
            """;
    }
}
