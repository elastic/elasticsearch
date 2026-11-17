/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Same-document point parity across native stage reads and the ordinary mapper-selected storage paths. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequencePointFieldsIT extends AbstractSequenceIntegTestCase {
    private static final List<String> SUFFIXES = List.of(
        "scalar",
        "list",
        "singleton",
        "coordinates",
        "missing",
        "null",
        "empty",
        "nullsub",
        "nulls"
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SpatialPlugin.class);
    }

    public void testStoredSourcePointsMatchOrdinaryWithAndWithoutDocValues() {
        for (boolean docValues : List.of(true, false)) {
            String index = "sequence-points-stored-" + docValues;
            createPoints(index, docValues, Settings.EMPTY);
            assertPointParity(index);
        }
    }

    public void testSyntheticSourcePointsMatchTheirOrdinaryRepresentation() {
        for (boolean docValues : List.of(true, false)) {
            String index = "sequence-points-synthetic-" + docValues;
            createPoints(index, docValues, Settings.builder().put("index.mapping.source.mode", "synthetic").build());
            assertPointParity(index);
        }
    }

    public void testStrictColumnarPointsMatchOrdinaryDocValuesBackedWkb() {
        String index = "sequence-points-columnar";
        createPoints(index, true, Settings.builder().put("index.mode", IndexMode.COLUMNAR.getName()).build());
        assertPointParity(index);
    }

    public void testPointPredicatesAndScalarDerivedKeysKeepRequiredInputs() {
        String index = "sequence-points-inputs";
        createPoints(index, true, Settings.EMPTY);
        for (String prefix : List.of("g", "c")) {
            try (var result = run(Strings.format("""
                SEQUENCE BY key WITHIN 30s PRIMARY 1
                (FROM %s | WHERE phase=="A" AND ST_X(%s_scalar)>0 | KEEP key),
                (FROM %s | WHERE phase=="B" | KEEP key)
                | KEEP key | LIMIT 1
                """, index, prefix, index))) {
                assertValues(result.values(), List.of(List.of("shared")));
            }
            try (
                var ordinary = run(
                    "FROM "
                        + index
                        + " | WHERE phase==\"A\" | EVAL normalized=TO_STRING("
                        + prefix
                        + "_nullsub) | KEEP normalized | LIMIT 1"
                );
                var sequence = run(Strings.format("""
                    SEQUENCE BY normalized WITHIN 30s PRIMARY 1
                    (FROM %s | WHERE phase=="A" | EVAL normalized=TO_STRING(%s_nullsub) | KEEP normalized),
                    (FROM %s | WHERE phase=="B" | EVAL normalized=TO_STRING(%s_nullsub) | KEEP normalized)
                    | KEEP normalized | LIMIT 1
                    """, index, prefix, index, prefix))
            ) {
                assertEquals(getValuesList(ordinary), getValuesList(sequence));
            }
        }
    }

    public void testShapesRemainUnsupportedAndUnusedColumnsCanBeDroppedOrPruned() {
        String index = "sequence-shape-boundary";
        Map<String, Object> properties = baseProperties();
        properties.put("g_shape", Map.of("type", "geo_shape"));
        properties.put("c_shape", Map.of("type", "shape"));
        createIndex(index, properties, Settings.EMPTY);
        indexPair(index, Map.of("g_shape", "POINT (1 2)", "c_shape", "POINT (3 4)"));
        String stages = "SEQUENCE BY key WITHIN 30s PRIMARY 1 (FROM "
            + index
            + " | WHERE phase==\"A\"), (FROM "
            + index
            + " | WHERE phase==\"B\")";
        for (String column : List.of("g_shape", "c_shape")) {
            var error = expectThrows(VerificationException.class, () -> run(stages + " | KEEP key," + column + " | LIMIT 1"));
            assertThat(error.getMessage(), containsString("cannot read source column [" + column + "]"));
            assertThat(error.getMessage(), containsString("DROP the unused column inside the stage"));
            assertThat(error.getMessage(), containsString("final KEEP"));
        }
        for (String query : List.of(
            stages + " | KEEP key | LIMIT 1",
            "SEQUENCE BY key WITHIN 30s PRIMARY 1 (FROM "
                + index
                + " | WHERE phase==\"A\" | DROP g_shape,c_shape), (FROM "
                + index
                + " | WHERE phase==\"B\" | DROP g_shape,c_shape) | KEEP key | LIMIT 1"
        )) {
            try (var result = run(query)) {
                assertValues(result.values(), List.of(List.of("shared")));
            }
        }
        var required = expectThrows(
            VerificationException.class,
            () -> run(
                "SEQUENCE BY key WITHIN 30s PRIMARY 1 (FROM "
                    + index
                    + " | WHERE phase==\"A\" | EVAL value=TO_STRING(g_shape) | WHERE LENGTH(value)>0 | KEEP key), (FROM "
                    + index
                    + " | WHERE phase==\"B\" | KEEP key) | KEEP key | LIMIT 1"
            )
        );
        assertThat(required.getMessage(), containsString("cannot read source column [g_shape]"));
    }

    /** The debug preference exposes packed local spatial values, which cannot be exported as ordinary point WKB. */
    public void testExplicitDocValuesPointProjectionBoundary() {
        String index = "sequence-points-doc-values-boundary";
        createPoints(index, true, Settings.EMPTY);
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put("field_extract_preference", "DOC_VALUES").build());
        var ordinary = expectThrows(Exception.class, () -> {
            try (
                var ignored = run(
                    syncEsqlQueryRequest("FROM " + index + " | WHERE phase==\"A\" | KEEP g_scalar,c_scalar | LIMIT 1").pragmas(pragmas)
                )
            ) {
                getValuesList(ignored);
                fail("Expected the ordinary direct-output spatial doc-values boundary");
            }
        });
        logger.info("Ordinary explicit DOC_VALUES point projection failure: {}", ExceptionsHelper.stackTrace(ordinary));
        var sequence = expectThrows(Exception.class, () -> {
            try (
                var ignored = run(
                    syncEsqlQueryRequest(
                        "SEQUENCE BY key WITHIN 30s PRIMARY 1 (FROM "
                            + index
                            + " | WHERE phase==\"A\"), (FROM "
                            + index
                            + " | WHERE phase==\"B\") | KEEP g_scalar,c_scalar | LIMIT 1"
                    ).pragmas(pragmas)
                )
            ) {
                fail("Expected SEQUENCE to refuse encoded point longs");
            }
        });
        assertThat(ExceptionsHelper.stackTrace(sequence), containsString("requires native WKB point bytes"));
    }

    private void assertPointParity(String index) {
        List<String> fields = new ArrayList<>();
        for (String prefix : List.of("g", "c")) {
            for (String suffix : SUFFIXES) {
                fields.add(prefix + "_" + suffix);
            }
        }
        String points = String.join(",", fields);
        for (String preference : List.of("NONE", "STORED")) {
            QueryPragmas pragmas = new QueryPragmas(Settings.builder().put("field_extract_preference", preference).build());
            for (int primary = 1; primary <= 2; primary++) {
                String phase = primary == 1 ? "A" : "B";
                String otherPhase = primary == 1 ? "B" : "A";
                int otherStage = 3 - primary;
                String member = fields.stream()
                    .map(field -> "other_" + field + "=MEMBER(" + otherStage + "," + field + ")")
                    .reduce((a, b) -> a + "," + b)
                    .orElseThrow();
                String memberColumns = String.join(",", fields.stream().map(field -> "other_" + field).toList());
                try (
                    var ordinary = run(
                        syncEsqlQueryRequest(
                            "FROM " + index + " | WHERE phase==\"" + phase + "\" | KEEP key,phase," + points + " | LIMIT 1"
                        ).pragmas(pragmas)
                    );
                    var other = run(
                        syncEsqlQueryRequest("FROM " + index + " | WHERE phase==\"" + otherPhase + "\" | KEEP " + points + " | LIMIT 1")
                            .pragmas(pragmas)
                    );
                    var sequence = run(
                        syncEsqlQueryRequest(
                            "SEQUENCE BY key WITHIN 30s PRIMARY "
                                + primary
                                + " (FROM "
                                + index
                                + " | WHERE phase==\"A\"), (FROM "
                                + index
                                + " | WHERE phase==\"B\") | EVAL "
                                + member
                                + " | KEEP key,phase,"
                                + points
                                + ","
                                + memberColumns
                                + " | LIMIT 1"
                        ).pragmas(pragmas)
                    )
                ) {
                    assertEquals(1, getValuesList(ordinary).size());
                    assertEquals(1, getValuesList(other).size());
                    assertEquals(1, getValuesList(sequence).size());
                    List<Object> expected = new ArrayList<>(getValuesList(ordinary).getFirst());
                    expected.addAll(getValuesList(other).getFirst());
                    assertEquals(index + "/" + preference + "/PRIMARY" + primary, List.of(expected), getValuesList(sequence));
                    List<Object> nativeExpected = new ArrayList<>(nativeRows(ordinary).getFirst());
                    nativeExpected.addAll(nativeRows(other).getFirst());
                    assertEquals(List.of(nativeExpected), nativeRows(sequence));
                    List<DataType> types = new ArrayList<>(ordinary.columns().stream().map(column -> column.type()).toList());
                    types.addAll(other.columns().stream().map(column -> column.type()).toList());
                    assertEquals(types, sequence.columns().stream().map(column -> column.type()).toList());
                }
            }
        }
    }

    private static List<List<Object>> nativeRows(EsqlQueryResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        for (var page : response.pages()) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> row = new ArrayList<>();
                for (int column = 0; column < page.getBlockCount(); column++) {
                    row.add(BlockUtils.toJavaObject(page.getBlock(column), position));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private void createPoints(String index, boolean docValues, Settings settings) {
        Map<String, Object> properties = baseProperties();
        for (String prefix : List.of("g", "c")) {
            for (String suffix : SUFFIXES) {
                Map<String, Object> mapping = new LinkedHashMap<>();
                mapping.put("type", prefix.equals("g") ? "geo_point" : "point");
                mapping.put("doc_values", docValues);
                if (suffix.equals("nullsub")) {
                    mapping.put("null_value", "POINT (7.123456789123 8.987654321987)");
                }
                properties.put(prefix + "_" + suffix, mapping);
            }
        }
        createIndex(index, properties, settings);
        var bulk = client().prepareBulk();
        for (String phase : List.of("A", "B")) {
            Map<String, Object> source = new LinkedHashMap<>();
            for (String prefix : List.of("g", "c")) {
                String first = phase.equals("A") ? "POINT (12.123456789123 -4.987654321987)" : "POINT (23.987654321987 5.123456789123)";
                String second = phase.equals("A") ? "POINT (-3.987654321987 8.123456789123)" : "POINT (-6.123456789123 9.987654321987)";
                source.put(prefix + "_scalar", first);
                source.put(prefix + "_list", List.of(second, first, second));
                source.put(prefix + "_singleton", List.of(first));
                source.put(prefix + "_coordinates", List.of(12.123456789123, -4.987654321987));
                source.put(prefix + "_null", null);
                source.put(prefix + "_empty", List.of());
                source.put(prefix + "_nullsub", null);
                source.put(prefix + "_nulls", Arrays.asList(second, null, first));
            }
            addEvent(bulk, index, phase, source);
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    private static Map<String, Object> baseProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("@timestamp", Map.of("type", "date_nanos"));
        properties.put("key", Map.of("type", "keyword"));
        properties.put("phase", Map.of("type", "keyword"));
        return properties;
    }

    private void createIndex(String index, Map<String, Object> properties, Settings settings) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(settings)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping(Map.of("properties", properties))
        );
    }

    private void indexPair(String index, Map<String, Object> values) {
        var bulk = client().prepareBulk();
        for (String phase : List.of("A", "B")) {
            addEvent(bulk, index, phase, new LinkedHashMap<>(values));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    private void addEvent(org.elasticsearch.action.bulk.BulkRequestBuilder bulk, String index, String phase, Map<String, Object> source) {
        source.put("@timestamp", phase.equals("A") ? "2026-09-25T21:14:51.389000001Z" : "2026-09-25T21:14:51.389000002Z");
        source.put("key", "shared");
        source.put("phase", phase);
        bulk.add(prepareIndex(index).setId(phase).setSource(source));
    }

    @After
    public void assertSequenceResourcesReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
        ensureBlocksReleased();
    }
}
