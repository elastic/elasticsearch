/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CohereRankedResponseEntityTests extends ESSingleNodeTestCase {

    public void testResponseLiteral() throws IOException {
        InferenceServiceResults parsedResults = CohereRankedResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseLiteral.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        MatcherAssert.assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(responseLiteralDocs));
    }

    public void testGeneratedResponse() throws IOException {
        int numDocs = randomIntBetween(1, 10);

        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>(numDocs);
        StringBuilder responseBuilder = new StringBuilder();

        responseBuilder.append("{");
        responseBuilder.append("\"id\":\"").append(randomAlphaOfLength(36)).append("\",");
        responseBuilder.append("\"results\": [");
        List<Integer> indices = linear(numDocs);
        List<Double> scores = linearDoubles(numDocs);
        for (int i = 0; i < numDocs; i++) {
            int index = indices.remove(randomInt(indices.size() - 1));

            responseBuilder.append("{");
            responseBuilder.append("\"index\":").append(index).append(",");
            responseBuilder.append("\"relevance_score\":").append(scores.get(i)).append("}");
            rankedDocs.add(new RankedDocsResults.RankedDoc(String.valueOf(index), String.valueOf(scores.get(i)), null));
            if (i < numDocs - 1) {
                responseBuilder.append(",");
            }
        }
        responseBuilder.append("],");
        responseBuilder.append("\"meta\": {");
        responseBuilder.append("\"api_version\": {");
        responseBuilder.append("\"version\": \"1\"},");
        responseBuilder.append("\"billed_units\": {");
        responseBuilder.append("\"search_units\":").append(randomIntBetween(1, 10)).append("}}}");

        InferenceServiceResults parsedResults = CohereRankedResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseBuilder.toString().getBytes(StandardCharsets.UTF_8))
        );
        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        MatcherAssert.assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(rankedDocs));
    }

    private final List<RankedDocsResults.RankedDoc> responseLiteralDocs = List.of(
        new RankedDocsResults.RankedDoc("2", "0.98005307", null),
        new RankedDocsResults.RankedDoc("3", "0.27904198", null),
        new RankedDocsResults.RankedDoc("0", "0.10194652", null)
    );

    private final String responseLiteral = """
        {
            "id": "d0760819-5a73-4d58-b163-3956d3648b62",
            "results": [
                {
                    "index": 2,
                    "relevance_score": 0.98005307
                },
                {
                    "index": 3,
                    "relevance_score": 0.27904198
                },
                {
                    "index": 0,
                    "relevance_score": 0.10194652
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

    public void testResponseLiteralWithDocuments() throws IOException {
        InferenceServiceResults parsedResults = CohereRankedResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseLiteralWithDocuments.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        MatcherAssert.assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(responseLiteralDocsWithText));
    }

    private final String responseLiteralWithDocuments = """
        {
            "id": "44873262-1315-4c06-8433-fdc90c9790d0",
            "results": [
                {
                    "document": {
                        "text": "Washington, D.C.."
                    },
                    "index": 2,
                    "relevance_score": 0.98005307
                },
                {
                    "document": {
                        "text": "Capital punishment has existed in the United States since beforethe United States was a country. "
                    },
                    "index": 3,
                    "relevance_score": 0.27904198
                },
                {
                    "document": {
                        "text": "Carson City is the capital city of the American state of Nevada."
                    },
                    "index": 0,
                    "relevance_score": 0.10194652
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

    private final List<RankedDocsResults.RankedDoc> responseLiteralDocsWithText = List.of(
        new RankedDocsResults.RankedDoc("2", "0.98005307", "Washington, D.C.."),
        new RankedDocsResults.RankedDoc(
            "3",
            "0.27904198",
            "Capital punishment has existed in the United States since beforethe United States was a country. "
        ),
        new RankedDocsResults.RankedDoc("0", "0.10194652", "Carson City is the capital city of the American state of Nevada.")
    );

    private ArrayList<Integer> linear(int n) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i <= n; i++) {
            list.add(i);
        }
        return list;
    }

    // creates a list of doubles of monotonically decreasing magnitude
    private ArrayList<Double> linearDoubles(int n) {
        ArrayList<Double> list = new ArrayList<>();
        double startValue = 1.0;
        double decrement = startValue / n + 1;
        for (int i = 0; i <= n; i++) {
            list.add(startValue - i * decrement);
        }
        return list;
    }

}
