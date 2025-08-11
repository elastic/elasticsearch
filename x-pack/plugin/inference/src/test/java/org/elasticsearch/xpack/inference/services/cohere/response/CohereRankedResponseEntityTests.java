/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CohereRankedResponseEntityTests extends ESTestCase {

    public void testResponseLiteral() throws IOException {
        InferenceServiceResults parsedResults = CohereRankedResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseLiteral.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        List<RankedDocsResults.RankedDoc> expected = responseLiteralDocs();
        for (int i = 0; i < ((RankedDocsResults) parsedResults).getRankedDocs().size(); i++) {
            assertEquals(((RankedDocsResults) parsedResults).getRankedDocs().get(i).index(), expected.get(i).index());
        }
    }

    public void testGeneratedResponse() throws IOException {
        int numDocs = randomIntBetween(1, 10);

        List<RankedDocsResults.RankedDoc> expected = new ArrayList<>(numDocs);
        StringBuilder responseBuilder = new StringBuilder();

        responseBuilder.append("{");
        responseBuilder.append("\"index\":\"").append(randomAlphaOfLength(36)).append("\",");
        responseBuilder.append("\"results\": [");
        List<Integer> indices = linear(numDocs);
        List<Float> scores = linearFloats(numDocs);
        for (int i = 0; i < numDocs; i++) {
            int index = indices.remove(randomInt(indices.size() - 1));

            responseBuilder.append("{");
            responseBuilder.append("\"index\":").append(index).append(",");
            responseBuilder.append("\"relevance_score\":").append(scores.get(i).toString()).append("}");
            expected.add(new RankedDocsResults.RankedDoc(index, scores.get(i), null));
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
            new HttpResult(mock(HttpResponse.class), responseBuilder.toString().getBytes(StandardCharsets.UTF_8))
        );
        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        for (int i = 0; i < ((RankedDocsResults) parsedResults).getRankedDocs().size(); i++) {
            assertEquals(((RankedDocsResults) parsedResults).getRankedDocs().get(i).index(), expected.get(i).index());
        }
    }

    private ArrayList<RankedDocsResults.RankedDoc> responseLiteralDocs() {
        var list = new ArrayList<RankedDocsResults.RankedDoc>();

        list.add(new RankedDocsResults.RankedDoc(2, 0.98005307F, null));
        list.add(new RankedDocsResults.RankedDoc(3, 0.27904198F, null));
        list.add(new RankedDocsResults.RankedDoc(0, 0.10194652F, null));
        return list;

    };

    private final String responseLiteral = """
        {
            "index": "d0760819-5a73-4d58-b163-3956d3648b62",
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
            new HttpResult(mock(HttpResponse.class), responseLiteralWithDocuments.getBytes(StandardCharsets.UTF_8))
        );

        MatcherAssert.assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        MatcherAssert.assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(responseLiteralDocsWithText));
    }

    private final String responseLiteralWithDocuments = """
        {
            "index": "44873262-1315-4c06-8433-fdc90c9790d0",
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
        new RankedDocsResults.RankedDoc(2, 0.98005307F, "Washington, D.C.."),
        new RankedDocsResults.RankedDoc(
            3,
            0.27904198F,
            "Capital punishment has existed in the United States since beforethe United States was a country. "
        ),
        new RankedDocsResults.RankedDoc(0, 0.10194652F, "Carson City is the capital city of the American state of Nevada.")
    );

    private ArrayList<Integer> linear(int n) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i <= n; i++) {
            list.add(i);
        }
        return list;
    }

    // creates a list of doubles of monotonically decreasing magnitude
    private ArrayList<Float> linearFloats(int n) {
        ArrayList<Float> list = new ArrayList<>();
        float startValue = 1.0f;
        float decrement = startValue / n + 1;
        for (int i = 0; i <= n; i++) {
            list.add(startValue - (i * decrement));
        }
        return list;
    }

}
