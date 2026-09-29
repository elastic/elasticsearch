/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.knneval.KnnEvalResponse.KnnSettingsResult;
import org.elasticsearch.xpack.knneval.KnnEvalResponse.ReportedSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class KnnEvalResponseTests extends AbstractWireSerializingTestCase<KnnEvalResponse> {

    @Override
    protected Writeable.Reader<KnnEvalResponse> instanceReader() {
        return KnnEvalResponse::new;
    }

    @Override
    protected KnnEvalResponse createTestInstance() {
        return new KnnEvalResponse(
            randomReportedSettings(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomVectorOpsKind(),
            randomList(3, KnnEvalResponseTests::randomResult),
            randomMap(0, 3, () -> Tuple.tuple(randomAlphaOfLength(8), new ElasticsearchException(randomAlphaOfLength(12))))
        );
    }

    @Override
    protected KnnEvalResponse mutateInstance(KnnEvalResponse instance) {
        ReportedSettings baseline = instance.getBaseline();
        long tookMs = instance.getBaselineTookMs();
        long vectorOps = instance.getBaselineVectorOps();
        String vectorOpsKind = instance.getBaselineVectorOpsKind();
        List<KnnSettingsResult> results = instance.getResults();
        Map<String, Exception> failures = instance.getFailures();
        switch (between(0, 5)) {
            case 0 -> baseline = randomValueOtherThan(baseline, KnnEvalResponseTests::randomReportedSettings);
            case 1 -> tookMs = randomValueOtherThan(tookMs, ESTestCase::randomNonNegativeLong);
            case 2 -> vectorOps = randomValueOtherThan(vectorOps, ESTestCase::randomNonNegativeLong);
            case 3 -> vectorOpsKind = randomValueOtherThan(vectorOpsKind, KnnEvalResponseTests::randomVectorOpsKind);
            case 4 -> results = CollectionUtils.appendToCopy(results, randomResult());
            case 5 -> {
                Map<String, Exception> mutated = new HashMap<>(failures);
                mutated.put(randomValueOtherThanMany(failures::containsKey, () -> randomAlphaOfLength(8)), new ElasticsearchException("x"));
                failures = mutated;
            }
            default -> throw new AssertionError("unexpected branch");
        }
        return new KnnEvalResponse(baseline, tookMs, vectorOps, vectorOpsKind, results, failures);
    }

    private static String randomVectorOpsKind() {
        return randomFrom(KnnEvalResponse.FULL_PRECISION_SCAN, KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE);
    }

    private static KnnSettingsResult randomResult() {
        return new KnnSettingsResult(
            randomReportedSettings(),
            randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    private static ReportedSettings randomReportedSettings() {
        KnnEvalSettings settings = randomBoolean()
            ? new KnnEvalSettings(null, null, null, true)
            : new KnnEvalSettings(
                randomBoolean() ? null : randomFloatBetween(0.0f, 100.0f, true),
                randomBoolean() ? null : randomIntBetween(1, 10_000),
                randomBoolean() ? null : randomFloatBetween(RescoreVectorBuilder.MIN_OVERSAMPLE, 10.0f, true),
                false
            );
        return new ReportedSettings(settings, randomBoolean());
    }

    public void testToXContent() throws IOException {
        String expected = """
            {
              "baseline": { "visit_percentage": 100.0 },
              "baseline_took_ms": 5,
              "baseline_vector_ops": 600,
              "baseline_vector_ops_kind": "quantized_visit_plus_rescore",
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
            KnnEvalResponse.ReportedSettings.of(new KnnEvalSettings(100.0f, null, null, false)),
            5,
            600,
            KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            List.of(
                new KnnEvalResponse.KnnSettingsResult(
                    new KnnEvalResponse.ReportedSettings(new KnnEvalSettings(20.0f, 200, null, false), true),
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
