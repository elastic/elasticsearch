/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void testKeepsResolutionWithoutNonTimeSeriesIndices() {
        EsIndex esIndex = exemplarsIndex(Map.of(".ds-exemplars-cpu-2026.10.08-000001", IndexMode.TIME_SERIES));

        assertSame(esIndex, ExemplarsRewriter.timeSeriesIndicesOnly(esIndex));
    }

    public void testDropsNonTimeSeriesIndices() {
        EsIndex esIndex = exemplarsIndex(
            Map.of(
                ".ds-exemplars-cpu-2026.10.01-000001",
                IndexMode.STANDARD,
                ".ds-exemplars-cpu-2026.10.08-000002",
                IndexMode.TIME_SERIES,
                "remote:.ds-exemplars-memory-2026.10.08-000001",
                IndexMode.LOGSDB
            )
        );

        EsIndex filtered = ExemplarsRewriter.timeSeriesIndicesOnly(esIndex);

        assertNotNull(filtered);
        assertEquals(Set.of(".ds-exemplars-cpu-2026.10.08-000002"), filtered.indexProperties().keySet());
        // The surviving index is local, so the remote cluster is left without one and the compute service skips it.
        assertEquals(Map.of("", List.of(".ds-exemplars-cpu-2026.10.08-000002")), filtered.concreteIndices());
        // The patterns to resolve shards from are untouched; the concrete indices select from what they resolve to.
        assertEquals(esIndex.originalIndices(), filtered.originalIndices());
        assertEquals(esIndex.name(), filtered.name());
        assertEquals(esIndex.mapping(), filtered.mapping());
        assertWarnings(
            "[exemplars] skipped [.ds-exemplars-cpu-2026.10.01-000001, remote:.ds-exemplars-memory-2026.10.08-000001]: "
                + "exemplars indices must be time series indices"
        );
    }

    public void testDropsResolutionWithoutAnyTimeSeriesIndex() {
        EsIndex esIndex = exemplarsIndex(Map.of(".ds-exemplars-cpu-2026.10.08-000001", IndexMode.STANDARD));

        assertNull(ExemplarsRewriter.timeSeriesIndicesOnly(esIndex));
        assertWarnings("[exemplars] skipped [.ds-exemplars-cpu-2026.10.08-000001]: exemplars indices must be time series indices");
    }

    /**
     * An index whose mappings resolved to nothing carries no index properties at all, so there is nothing to filter and nothing to
     * warn about.
     */
    public void testKeepsResolutionWithoutIndexProperties() {
        EsIndex esIndex = exemplarsIndex(Map.of());

        assertSame(esIndex, ExemplarsRewriter.timeSeriesIndicesOnly(esIndex));
    }

    private static IndexResolution resolution(Map<String, List<String>> concreteIndices) {
        return IndexResolution.valid(new EsIndex("metrics-*", Map.of(), Map.of(), concreteIndices, concreteIndices));
    }

    /**
     * An exemplars resolution over the given concrete indices, named as {@code _field_caps} reports them: qualified with the cluster
     * alias in the index properties and unqualified under it in the concrete indices.
     */
    private static EsIndex exemplarsIndex(Map<String, IndexMode> indexModes) {
        Map<String, IndexProperties> indexProperties = new HashMap<>();
        Map<String, List<String>> concreteIndices = new HashMap<>();
        indexModes.forEach((index, mode) -> {
            indexProperties.put(index, new IndexProperties(mode, 1));
            var split = RemoteClusterAware.splitIndexName(index);
            concreteIndices.computeIfAbsent(split.getClusterGroupingKey(), cluster -> new ArrayList<>()).add(split.indexExpression());
        });
        EsField value = new EsField("value", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        return new EsIndex("exemplars-*", Map.of("value", value), indexProperties, Map.of("", List.of("exemplars-*")), concreteIndices);
    }
}
