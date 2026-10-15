/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.List;
import java.util.Map;

public class ExemplarsRewriterTests extends ESTestCase {

    public void testDerivesExemplarDataStreamsFromBackingIndices() {
        IndexResolution resolution = resolution(
            Map.of(
                "",
                List.of(
                    ".ds-metrics-cpu-2026.10.01-000001",
                    ".ds-metrics-cpu-2026.10.08-000002",
                    "partial-restored-.ds-metrics-memory-2026.10.08-000001",
                    ".fs-metrics-errors-2026.10.08-000001"
                )
            )
        );

        assertEquals("exemplars-cpu,exemplars-errors,exemplars-memory", ExemplarsRewriter.exemplarIndexPattern(List.of(resolution)));
    }

    public void testDerivesCrossClusterExemplarDataStreams() {
        IndexResolution firstResolution = resolution(
            Map.of("", List.of(".ds-metrics-cpu-2026.10.08-000001"), "remote-a", List.of(".ds-metrics-cpu-2026.10.08-000001"))
        );
        IndexResolution secondResolution = resolution(
            Map.of(
                "remote-a",
                List.of(".ds-metrics-cpu-2026.10.01-000001"),
                "remote-b",
                List.of("remote-b:.ds-metrics-memory-2026.10.08-000001")
            )
        );

        assertEquals(
            "exemplars-cpu,remote-a:exemplars-cpu,remote-b:exemplars-memory",
            ExemplarsRewriter.exemplarIndexPattern(List.of(firstResolution, secondResolution))
        );
    }

    public void testIgnoresNamesThatAreNotBackingIndices() {
        IndexResolution resolution = resolution(
            Map.of(
                "",
                List.of("metrics-standalone", "custom-backing-index", ".ds-logs-app-2026.10.08-000001"),
                "remote",
                List.of("metrics-remote-standalone")
            )
        );

        assertNull(ExemplarsRewriter.exemplarIndexPattern(List.of(resolution)));
    }

    public void testIgnoresInvalidAndEmptyResolutions() {
        assertNull(
            ExemplarsRewriter.exemplarIndexPattern(
                List.of(IndexResolution.notFound("metrics-missing"), IndexResolution.empty("metrics-empty"))
            )
        );
    }

    private static IndexResolution resolution(Map<String, List<String>> concreteIndices) {
        return IndexResolution.valid(new EsIndex("metrics-*", Map.of(), Map.of(), concreteIndices, concreteIndices));
    }
}
