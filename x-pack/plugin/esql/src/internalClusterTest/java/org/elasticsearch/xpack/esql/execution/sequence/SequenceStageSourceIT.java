/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Tests explicit ordinary primary source columns for stored, synthetic and disabled source. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceStageSourceIT extends AbstractSequenceIntegTestCase {
    private static final String FIRST = "sequence-stage-source-a";
    private static final String SECOND = "sequence-stage-source-b";
    private static final String THIRD = "sequence-stage-source-c";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final String SEQUENCE = """
        SEQUENCE BY key WITHIN 30s PRIMARY %s
        (FROM sequence-stage-source-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
        (FROM sequence-stage-source-b METADATA _id, _index, _source | KEEP key, _id, _index, _source)
        """;

    /** An explicit third primary supplies ordinary source columns even when member IDs are identical. */
    public void testThirdPrimaryMemberSuppliesOrdinarySourceColumns() {
        createIndex(FIRST);
        createIndex(SECOND);
        createIndex(THIRD);
        Map<String, Object> first = source(BASE, "shared", "openssl", "alice");
        Map<String, Object> second = source(BASE.plusNanos(1), "shared", "tar", "bob");
        Map<String, Object> third = source(BASE.plusNanos(2), "shared", "upload", "carol");
        seed(FIRST, "same-id", first);
        seed(SECOND, "same-id", second);
        seed(THIRD, "same-id", third);

        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 3
            (FROM sequence-stage-source-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
            (FROM sequence-stage-source-b METADATA _id, _index, _source | KEEP key, _id, _index, _source),
            (FROM sequence-stage-source-c METADATA _id, _index, _source | KEEP key, _id, _index, _source)
            | RENAME _source AS primary_source, _index AS primary_index
            | KEEP key, primary_index, primary_source
            | LIMIT 1
            """;
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertEquals(List.of(List.of("shared", THIRD, third)), getValuesList(response));
        }
    }

    /** Synthetic source keeps ordinary FROM semantics and must not be confused with the original submitted document. */
    public void testSyntheticStageSourceMatchesOrdinaryReconstruction() {
        createIndex(FIRST, true, "synthetic");
        createIndex(SECOND);
        Map<String, Object> first = source(BASE, "shared", "first", "alice");
        first.put("payload", List.of("z", "a", "z"));
        Map<String, Object> second = source(BASE.plusNanos(1), "shared", "second", "bob");
        seed(FIRST, "A", first);
        seed(SECOND, "B", second);
        Object reconstructed;
        try (var response = run(syncEsqlQueryRequest("FROM " + FIRST + " METADATA _source | KEEP _source | LIMIT 1"))) {
            List<List<Object>> rows = getValuesList(response);
            assertEquals(1, rows.size());
            reconstructed = rows.getFirst().getFirst();
        }
        assertTrue(reconstructed instanceof Map<?, ?>);
        assertEquals(List.of("a", "z"), ((Map<?, ?>) reconstructed).get("payload"));
        assertNotEquals(first, reconstructed);
        for (int primary : List.of(1, 2)) {
            try (var response = run(syncEsqlQueryRequest(SEQUENCE.formatted(primary) + " | KEEP key,_source | LIMIT 1"))) {
                assertEquals(List.of(List.of("shared", primary == 1 ? reconstructed : second)), getValuesList(response));
            }
        }
    }

    /** Disabled source retains ordinary null semantics even though the selected document still has an identity. */
    public void testDisabledStageSourceMatchesOrdinaryNull() {
        createIndex(FIRST, false, "stored");
        createIndex(SECOND);
        seed(FIRST, "A", source(BASE, "shared", "first", "alice"));
        Map<String, Object> second = source(BASE.plusNanos(1), "shared", "second", "bob");
        seed(SECOND, "B", second);
        try (var response = run(syncEsqlQueryRequest("FROM " + FIRST + " METADATA _source | KEEP _source | LIMIT 1"))) {
            List<List<Object>> rows = getValuesList(response);
            assertEquals(1, rows.size());
            assertNull(rows.getFirst().getFirst());
        }
        for (int primary : List.of(1, 2)) {
            try (var response = run(syncEsqlQueryRequest(SEQUENCE.formatted(primary) + " | KEEP key,_source | LIMIT 1"))) {
                assertEquals(List.of(Arrays.asList("shared", primary == 1 ? null : second)), getValuesList(response));
            }
        }
    }

    /** Expanded analytical stages retain the selected original through ordinary metadata without enabling retained-match expansion. */
    public void testExpandedStagesExposeSelectedPrimaryOriginal() {
        createIndex(FIRST);
        createIndex(SECOND);
        Map<String, Object> first = source(BASE, List.of("shared", "other"), "first", "alice");
        Map<String, Object> second = source(BASE.plusNanos(1), "shared", "second", "bob");
        seed(FIRST, "A", first);
        seed(SECOND, "B", second);
        for (int primary : List.of(1, 2)) {
            try (var response = run("""
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM sequence-stage-source-a METADATA _source | MV_EXPAND key | KEEP key, _source),
                (FROM sequence-stage-source-b METADATA _source | MV_EXPAND key | KEEP key, _source)
                | KEEP key, _source | LIMIT 1
                """.formatted(primary))) {
                assertEquals(List.of(List.of("shared", primary == 1 ? first : second)), getValuesList(response));
            }
        }
    }

    private void createIndex(String index) {
        createIndex(index, true, "stored");
    }

    private void createIndex(String index, boolean sourceEnabled, String sourceMode) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    indexSettings(1, 0).put("index.mapping.source.mode", sourceMode).put("index.mapping.synthetic_source_keep", "none")
                )
                .setMapping("""
                    {"dynamic":false,"_source":{"enabled":%s},"properties":{
                      "@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},
                      "payload":{"type":"keyword"},"number":{"type":"long"},
                      "process":{"properties":{"name":{"type":"keyword"}}},
                      "user":{"properties":{"name":{"type":"keyword"}}}
                    }}
                    """.formatted(sourceEnabled))
        );
        ensureGreen(index);
    }

    private void seed(String index, String id, Map<String, Object> source) {
        prepareIndex(index).setId(id).setSource(source).get();
        refresh(index);
    }

    private static Map<String, Object> source(Instant timestamp, Object key, String processName, String userName) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("@timestamp", timestamp.toString());
        source.put("key", key);
        source.put("process", Map.of("name", processName, "entity_id", processName + "-entity"));
        source.put("user", Map.of("name", userName));
        return source;
    }

    /** Checks that sequence execution releases its point-in-time contexts and ES|QL blocks after each test. */
    @After
    public void checkSequenceResourcesReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

}
