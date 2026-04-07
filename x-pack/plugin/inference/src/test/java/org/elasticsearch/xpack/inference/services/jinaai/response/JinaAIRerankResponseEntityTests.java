/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JinaAIRerankResponseEntityTests extends ESTestCase {

    private static final String WASHINGTON_TEXT = "Washington, D.C..";
    private static final String CAPITAL_PUNISHMENT_TEXT =
        "Capital punishment has existed in the United States since before the United States was a country. ";
    private static final String CARSON_CITY_TEXT = "Carson City is the capital city of the American state of Nevada.";

    private static final List<RankedDocsResults.RankedDoc> RESPONSE_LITERAL_DOCS_WITH_TEXT = List.of(
        new RankedDocsResults.RankedDoc(2, 0.98005307F, WASHINGTON_TEXT),
        new RankedDocsResults.RankedDoc(3, 0.27904198F, CAPITAL_PUNISHMENT_TEXT),
        new RankedDocsResults.RankedDoc(0, 0.10194652F, CARSON_CITY_TEXT)
    );

    public void testResponseLiteral() throws IOException {
        String responseLiteral = """
            {
                "model": "model",
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
                "usage": {
                    "total_tokens": 15
                }
            }
            """;
        InferenceServiceResults parsedResults = JinaAIRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseLiteral.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(RankedDocsResults.class));
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
        responseBuilder.append("\"model\": \"model\",");
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
        responseBuilder.append("\"usage\": {");
        responseBuilder.append("\"total_tokens\": 15}");
        responseBuilder.append("}");

        InferenceServiceResults parsedResults = JinaAIRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseBuilder.toString().getBytes(StandardCharsets.UTF_8))
        );
        assertThat(parsedResults, instanceOf(RankedDocsResults.class));
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

    }

    public void testResponseLiteralWithDocuments() throws IOException {
        String responseLiteralWithDocuments = Strings.format("""
            {
                "model": "model",
                "results": [
                    {
                        "document": {
                            "text": "%s"
                        },
                        "index": 2,
                        "relevance_score": 0.98005307
                    },
                    {
                        "document": {
                            "text": "%s"
                        },
                        "index": 3,
                        "relevance_score": 0.27904198
                    },
                    {
                        "document": {
                            "text": "%s"
                        },
                        "index": 0,
                        "relevance_score": 0.10194652
                    }
                ],
                "usage": {
                    "total_tokens": 15
                }
            }
            """, WASHINGTON_TEXT, CAPITAL_PUNISHMENT_TEXT, CARSON_CITY_TEXT);
        InferenceServiceResults parsedResults = JinaAIRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseLiteralWithDocuments.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(RESPONSE_LITERAL_DOCS_WITH_TEXT));
    }

    public void testResponseLiteralWithDocumentsAsString() throws IOException {
        String responseLiteralWithDocuments = Strings.format("""
            {
                "model": "model",
                "results": [
                    {
                        "document": "%s",
                        "index": 2,
                        "relevance_score": 0.98005307
                    },
                    {
                        "document": "%s",
                        "index": 3,
                        "relevance_score": 0.27904198
                    },
                    {
                        "document": "%s",
                        "index": 0,
                        "relevance_score": 0.10194652
                    }
                ],
                "usage": {
                    "total_tokens": 15
                }
            }
            """, WASHINGTON_TEXT, CAPITAL_PUNISHMENT_TEXT, CARSON_CITY_TEXT);
        InferenceServiceResults parsedResults = JinaAIRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseLiteralWithDocuments.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(RESPONSE_LITERAL_DOCS_WITH_TEXT));
    }

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
