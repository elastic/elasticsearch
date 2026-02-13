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
    private static final String RESPONSE_JSON_TWO_DOCS = """
        [
            {
              "index": 4,
              "score": -0.22222222222222222,
              "text": "ranked second"
            },
            {
              "index": 1,
              "score": 1.11111111111111111,
              "text": "ranked first"
            }
        ]
        """;
    private static final List<RankedDocsResultsTests.RerankExpectation> EXPECTED_TWO_DOCS = List.of(
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 1, "relevance_score", 1.11111111111111111F, "text", "ranked first")),
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 4, "relevance_score", -0.22222222222222222F, "text", "ranked second"))
    );

    private static final String RESPONSE_JSON_FIVE_DOCS = """
        [
            {
              "index": 1,
              "score": 1.11111111111111111,
              "text": "ranked first"
            },
            {
              "index": 3,
              "score": -0.33333333333333333,
              "text": "ranked third"
            },
            {
              "index": 0,
              "score": -0.55555555555555555,
              "text": "ranked fifth"
            },
            {
              "index": 2,
              "score": -0.44444444444444444,
              "text": "ranked fourth"
            },
            {
              "index": 4,
              "score": -0.22222222222222222,
              "text": "ranked second"
            }
        ]
        """;
    private static final List<RankedDocsResultsTests.RerankExpectation> EXPECTED_FIVE_DOCS = List.of(
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 1, "relevance_score", 1.11111111111111111F, "text", "ranked first")),
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 4, "relevance_score", -0.22222222222222222F, "text", "ranked second")),
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 3, "relevance_score", -0.33333333333333333F, "text", "ranked third")),
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 2, "relevance_score", -0.44444444444444444F, "text", "ranked fourth")),
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 0, "relevance_score", -0.55555555555555555F, "text", "ranked fifth"))
    );

    private static final HuggingFaceRerankRequest REQUEST_MOCK = mock(HuggingFaceRerankRequest.class);

    public void testFromResponse_CreatesRankedDocsResults_TopNNull_FiveDocs_NoLimit() throws IOException {
        assertTopNLimit(null, RESPONSE_JSON_FIVE_DOCS, EXPECTED_FIVE_DOCS);
    }

    public void testFromResponse_CreatesRankedDocsResults_TopN5_TwoDocs_NoLimit() throws IOException {
        assertTopNLimit(5, RESPONSE_JSON_TWO_DOCS, EXPECTED_TWO_DOCS);
    }

    public void testFromResponse_CreatesRankedDocsResults_TopN2_FiveDocs_Limits() throws IOException {
        assertTopNLimit(2, RESPONSE_JSON_FIVE_DOCS, EXPECTED_TWO_DOCS);
    }

    public void testFails_CreateRankedDocsResults_IndexFieldNull() {
        String responseJson = """
            [
                {
                  "score": 1.11111111111111111,
                  "text": "ranked first"
                }
            ]
            """;
        assertMissingFieldThrowsIllegalArgumentException(responseJson, MISSED_FIELD_INDEX);
    }

    public void testFails_CreateRankedDocsResults_ScoreFieldNull() {
        String responseJson = """
            [
                {
                  "index": 1,
                  "text": "ranked first"
                }
            ]
            """;
        assertMissingFieldThrowsIllegalArgumentException(responseJson, MISSED_FIELD_SCORE);
    }

    private void assertMissingFieldThrowsIllegalArgumentException(String responseJson, String missingField) {
        when(REQUEST_MOCK.getTopN()).thenReturn(1);

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceRerankResponseEntity.fromResponse(
                REQUEST_MOCK,
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );
        assertThat(thrownException.getMessage(), is("Required [" + missingField + "]"));
    }

    private void assertTopNLimit(Integer topN, String responseJson, List<RankedDocsResultsTests.RerankExpectation> expectation)
        throws IOException {
        when(REQUEST_MOCK.getTopN()).thenReturn(topN);

        RankedDocsResults parsedResults = HuggingFaceRerankResponseEntity.fromResponse(
            REQUEST_MOCK,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(parsedResults.asMap(), is(buildExpectationRerank(expectation)));
    }
}
