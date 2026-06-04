/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class PreAnalyzerTests extends ESTestCase {

    public void testCollectInferenceIds() {
        PreAnalyzer preAnalyzer = new PreAnalyzer();

        // Rerank inference plan
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | RERANK \"italian food recipe\" ON title WITH { \"inference_id\": \"rerank-inference-id\" }",
            List.of("rerank-inference-id")
        );

        // Completion inference plan
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | COMPLETION \"italian food recipe\" WITH { \"inference_id\": \"completion-inference-id\" }",
            List.of("completion-inference-id")
        );

        // Text embedding function
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING(\"description\", \"text-embedding-inference-id\")",
            List.of("text-embedding-inference-id")
        );

        // Embedding function
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = EMBEDDING(\"description\", \"embedding-inference-id\")",
            List.of("embedding-inference-id")
        );

        // Nested inference functions
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING(TEXT_EMBEDDING(\"nested\", \"nested-id\"), \"outer-id\")",
            List.of("nested-id", "outer-id")
        );

        // Inference function wrapping a regular (non-inference) function: the cheap short-circuit must
        // skip CONCAT but still collect the inference function's id.
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING(CONCAT(\"a\", \"b\"), \"text-embedding-inference-id\")",
            List.of("text-embedding-inference-id")
        );

        // Multiple inference plans
        assertCollectInferenceIds(preAnalyzer, """
            FROM books METADATA _score
            | RERANK "italian food recipe" ON title WITH { "inference_id": "rerank-inference-id" }
            | COMPLETION "italian food recipe" WITH { "inference_id": "completion-inference-id" }
            """, List.of("rerank-inference-id", "completion-inference-id"));

        // No inference operations
        assertCollectInferenceIds(preAnalyzer, "FROM books | WHERE title:\"test\"", List.of());

        // No inference operations, but several regular functions are present: the cheap short-circuit must
        // skip every one of them without collecting any inference id.
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books | EVAL x = LENGTH(CONCAT(TO_LOWER(title), \"!\")) | WHERE x > ABS(-1)",
            List.of()
        );
    }

    private void assertCollectInferenceIds(PreAnalyzer preAnalyzer, String query, List<String> expectedInferenceIds) {
        List<String> inferenceIds = preAnalyzer.preAnalyze(TEST_PARSER.parseQuery(query)).inferenceIds();
        assertThat(inferenceIds, containsInAnyOrder(expectedInferenceIds.toArray(new String[0])));
    }
}
