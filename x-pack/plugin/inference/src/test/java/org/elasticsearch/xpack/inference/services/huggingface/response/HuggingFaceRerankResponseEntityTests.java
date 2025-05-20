/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.huggingface.request.rerank.HuggingFaceRerankRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.buildExpectationRerank;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HuggingFaceRerankResponseEntityTests extends ESTestCase {
    private static final String MISSED_FIELD_INDEX = "index";
    private static final String MISSED_FIELD_SCORE = "score";

    public void testFromResponse_CreatesRankedDocsResults() throws IOException {
        String responseJson = """
            [
                {
                  "index": 0,
                  "score": -0.07996220886707306,
                  "text": "luke"
                }
            ]
            """;
        HuggingFaceRerankRequest huggingFaceRerankRequestMock = mock(HuggingFaceRerankRequest.class);
        when(huggingFaceRerankRequestMock.getTopN()).thenReturn(1);

        RankedDocsResults parsedResults = HuggingFaceRerankResponseEntity.fromResponse(
            huggingFaceRerankRequestMock,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationRerank(
                    List.of(
                        new RankedDocsResultsTests.RerankExpectation(
                            Map.of("index", 0, "relevance_score", -0.07996220886707306F, "text", "luke")
                        )
                    )
                )
            )
        );
    }

    public void testFails_CreateRankedDocsResults_IndexFieldNull() {
        String responseJson = """
            [
                {
                  "score": -0.07996220886707306,
                  "text": "luke"
                }
            ]
            """;
        assertMissingFieldThrowsIllegalArgumentException(responseJson, MISSED_FIELD_INDEX);
    }

    public void testFails_CreateRankedDocsResults_ScoreFieldNull() {
        String responseJson = """
            [
                {
                  "index": 0,
                  "text": "luke"
                }
            ]
            """;
        assertMissingFieldThrowsIllegalArgumentException(responseJson, MISSED_FIELD_SCORE);
    }

    private void assertMissingFieldThrowsIllegalArgumentException(String responseJson, String missingField) {
        HuggingFaceRerankRequest huggingFaceRerankRequestMock = mock(HuggingFaceRerankRequest.class);
        when(huggingFaceRerankRequestMock.getTopN()).thenReturn(1);

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceRerankResponseEntity.fromResponse(
                huggingFaceRerankRequestMock,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
        assertThat(thrownException.getMessage(), is("Required [%s]".formatted(missingField)));
    }
}
