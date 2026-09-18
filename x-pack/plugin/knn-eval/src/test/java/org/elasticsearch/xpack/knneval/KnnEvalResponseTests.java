/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

/** Tests response serialization and JSON rendering. */
public class KnnEvalResponseTests extends ESTestCase {

    public void testResponseSerialization() throws IOException {
        KnnEvalResponse original = response();
        KnnEvalResponse copy = copyWriteable(original, new NamedWriteableRegistry(List.of()), KnnEvalResponse::new);
        assertNotSame(original, copy);
        assertEquals(original.getBaseline(), copy.getBaseline());
        assertEquals(original.getBaselineTookMs(), copy.getBaselineTookMs());
        assertEquals(original.getBaselineVectorOps(), copy.getBaselineVectorOps());
        assertEquals(original.getBaselineVectorOpsKind(), copy.getBaselineVectorOpsKind());
        assertEquals(original.getMaxQueriesPerBatch(), copy.getMaxQueriesPerBatch());
        assertEquals(original.getResults(), copy.getResults());
        assertEquals(original.getFailures().keySet(), copy.getFailures().keySet());
    }

    public void testToXContent() throws IOException {
        String expected = """
            {
              "baseline": { "visit_percentage": 100.0 },
              "baseline_took_ms": 5,
              "baseline_vector_ops": 600,
              "baseline_vector_ops_kind": "quantized_visit_plus_rescore",
              "max_queries_per_batch": 7,
              "results": [
                {
                  "knn_settings": {
                    "visit_percentage": 20.0, "num_candidates": 200,
                    "rescore_window_capped": true
                  },
                  "recall": 0.5,
                  "included_queries": 1,
                  "excluded_queries": 0,
                  "took_ms": 2,
                  "vector_ops": 40
                }
              ],
              "failures": {
                "q2": {
                  "error": {
                    "root_cause": [ { "type": "parsing_exception", "reason": "no such field", "line": 1, "col": 2 } ],
                    "type": "parsing_exception", "reason": "no such field", "line": 1, "col": 2
                  }
                }
              }
            }""";
        assertToXContentEquivalent(
            new BytesArray(expected),
            toXContent(response(), XContentType.JSON, ToXContent.EMPTY_PARAMS, false),
            XContentType.JSON
        );
    }

    private static KnnEvalResponse response() {
        return new KnnEvalResponse(
            KnnEvalResponse.ReportedKnobs.of(new KnnEvalKnobs(100.0f, null, null, false)),
            5,
            600,
            KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            7,
            List.of(
                new KnnEvalResponse.KnnSettingsResult(
                    new KnnEvalResponse.ReportedKnobs(new KnnEvalKnobs(20.0f, 200, null, false), true),
                    0.5,
                    1,
                    0,
                    2,
                    40
                )
            ),
            Map.of("q2", new ParsingException(1, 2, "no such field", null))
        );
    }
}
