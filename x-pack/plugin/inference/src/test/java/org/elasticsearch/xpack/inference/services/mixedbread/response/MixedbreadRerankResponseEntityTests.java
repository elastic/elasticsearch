/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MixedbreadRerankResponseEntityTests extends ESTestCase {

    private static final String HARPER_LEE = "Harper Lee, an American novelist";
    private static final String NOVEL_BY_HARPER_LEE = "To Kill a Mockingbird is a novel by Harper Lee";
    private static final String JANE_AUSTEN = "Jane Austen was an English novelist";

    private static final List<RankedDocsResults.RankedDoc> RESPONSE_LITERAL_DOCS = List.of(
        new RankedDocsResults.RankedDoc(0, 0.98291015625F, null),
        new RankedDocsResults.RankedDoc(2, 0.61962890625F, null),
        new RankedDocsResults.RankedDoc(3, 0.3642578125F, null)
    );

    private static final List<RankedDocsResults.RankedDoc> RESPONSE_LITERAL_DOCS_WITH_TEXT = List.of(
        new RankedDocsResults.RankedDoc(0, 0.98291015625F, HARPER_LEE),
        new RankedDocsResults.RankedDoc(2, 0.61962890625F, NOVEL_BY_HARPER_LEE),
        new RankedDocsResults.RankedDoc(3, 0.3642578125F, JANE_AUSTEN)
    );

    public void testResponseLiteral() throws IOException {

        InferenceServiceResults parsedResults = MixedbreadRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), RESPONSE_LITERAL.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(RESPONSE_LITERAL_DOCS));
    }

    public void testResponseLiteralWithDocumentsAsString() throws IOException {
        InferenceServiceResults parsedResults = MixedbreadRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), RESPONSE_LITERAL_WITH_INPUT.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(parsedResults, instanceOf(RankedDocsResults.class));
        assertThat(((RankedDocsResults) parsedResults).getRankedDocs(), is(RESPONSE_LITERAL_DOCS_WITH_TEXT));
    }

    private static final String RESPONSE_LITERAL = """
        {
            "usage": {
                    "prompt_tokens": 162,
                    "total_tokens": 162,
                    "completion_tokens": 0
            },
            "model": "mixedbread-ai/mxbai-rerank-xsmall-v1",
            "data": [
                {
                    "index": 0,
                    "score": 0.98291015625,
                    "object": "rank_result"
                },
                {
                    "index": 2,
                    "score": 0.61962890625,
                    "object": "rank_result"
                },
                {
                    "index": 3,
                    "score": 0.3642578125,
                    "object": "rank_result"
                }
            ],
            "object": "list",
            "top_k": 3,
            "return_input": false
        }
        """;

    private static final String RESPONSE_LITERAL_WITH_INPUT = Strings.format("""
        {
            "usage": {
                    "prompt_tokens": 162,
                    "total_tokens": 162,
                    "completion_tokens": 0
            },
            "model": "mixedbread-ai/mxbai-rerank-xsmall-v1",
            "data": [
                {
                    "index": 0,
                    "score": 0.98291015625,
                    "input": "%s",
                    "object": "rank_result"
                },
                {
                    "index": 2,
                    "score": 0.61962890625,
                    "input": "%s",
                    "object": "rank_result"
                },
                {
                    "index": 3,
                    "score": 0.3642578125,
                    "input": "%s",
                    "object": "rank_result"
                }
            ],
            "object": "list",
            "top_k": 3,
            "return_input": false
        }
        """, HARPER_LEE, NOVEL_BY_HARPER_LEE, JANE_AUSTEN);
}
