/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.After;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Tests original member documents carried by ordinary typed stage source columns. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceStageSourceIT extends AbstractEsqlIntegTestCase {
    private static final String FIRST = "sequence-stage-source-a";
    private static final String SECOND = "sequence-stage-source-b";
    private static final String THIRD = "sequence-stage-source-c";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final String SEQUENCE = """
        SEQUENCE BY key WITH {"maxspan":"30s"}
        (FROM sequence-stage-source-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
        (FROM sequence-stage-source-b METADATA _id, _index, _source | KEEP key, _id, _index, _source)
        """;

    /** Original source structure survives even when typed stage values are changed, renamed or dropped. */
    public void testStageSourceKeepsOriginalMembers() {
        createIndex(FIRST);
        createIndex(SECOND);
        Map<String, Object> first = source(BASE, "shared", "openssl", "alice");
        first.put("payload", "first");
        first.put("number", 9007199254740993L);
        Map<String, Object> unused = new LinkedHashMap<>();
        unused.put("array", Arrays.asList("duplicate", null, "duplicate"));
        unused.put("object", Map.of("nested", List.of(Map.of("value", "one"), Map.of("value", "two"))));
        unused.put("explicit_null", null);
        first.put("unused", unused);
        first.put("event", Map.of("kind", "event", "action", "inert-encryption"));
        Map<String, Object> second = source(BASE.plusNanos(2), "shared", "tar", "bob");
        second.put("payload", "second");
        second.put("null_field", null);
        seed(FIRST, "A", first);
        seed(SECOND, "B", second);

        String query = """
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-stage-source-a METADATA _id, _index, _source
             | EVAL process.name = "computed-openssl" | RENAME payload AS renamed | DROP user.name
             | KEEP key, process.name, renamed, _id, _index, _source),
            (FROM sequence-stage-source-b METADATA _id, _index, _source
             | EVAL process.name = "computed-archiver" | KEEP key, process.name, _id, _index, _source)
            | WHERE stage2.process.name == "computed-archiver"
            | KEEP key, stage1.process.name, stage2.process.name,
                   stage1._index, stage1._id, stage1._source, stage2._index, stage2._id, stage2._source
            """;
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertEquals(1, rows.size());
            assertTrue(rows.getFirst().get(5) instanceof Map<?, ?>);
            assertTrue(rows.getFirst().get(8) instanceof Map<?, ?>);
            assertEquals(List.of(List.of("shared", "computed-openssl", "computed-archiver", FIRST, "A", first, SECOND, "B", second)), rows);
        }
    }

    /** A consumer can choose the third member using ordinary columns, including when member IDs are identical. */
    public void testPrimaryMemberSelectedByColumn() {
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
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM sequence-stage-source-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
            (FROM sequence-stage-source-b METADATA _id, _index, _source | KEEP key, _id, _index, _source),
            (FROM sequence-stage-source-c METADATA _id, _index, _source | KEEP key, _id, _index, _source)
            | RENAME stage3._source AS primary_source, stage3._index AS primary_index
            | KEEP key, primary_index, primary_source, stage1._source, stage2._source
            """;
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertEquals(List.of(List.of("shared", THIRD, third, first, second)), getValuesList(response));
        }
    }

    /** Source payloads are replayed only for selected members and are pruned from key-only and count-only queries. */
    public void testStageSourceLoadedOnlyForSelectedMembers() {
        createIndex(FIRST);
        createIndex(SECOND);
        for (int i = 0; i < 20; i++) {
            String key = "key-" + (100 + i);
            prepareIndex(FIRST).setId("a-" + i).setSource(source(BASE.plusSeconds(i * 100L), key, "first", "alice")).get();
            prepareIndex(SECOND).setId("b-" + i)
                .setSource(source(BASE.plusSeconds(i * 100L + 1), i % 4 == 0 ? key : "other-" + i, "second", "bob"))
                .get();
        }
        refresh(FIRST, SECOND);

        Measured withSource = measure("with_source", SEQUENCE + " | KEEP key, stage1._source, stage2._source | SORT key | LIMIT 100");
        Measured keysOnly = measure("keys_only", SEQUENCE + " | KEEP key | SORT key | LIMIT 100");
        Measured countOnly = measure("count_only", SEQUENCE + " | STATS c = COUNT(*)");
        assertEquals(5, withSource.values().size());
        assertEquals(5, keysOnly.values().size());
        assertEquals(List.of(List.of(5L)), countOnly.values());
        for (int i = 0; i < 5; i++) {
            int candidate = i * 4;
            String key = "key-" + (100 + candidate);
            assertEquals(
                List.of(
                    key,
                    source(BASE.plusSeconds(candidate * 100L), key, "first", "alice"),
                    source(BASE.plusSeconds(candidate * 100L + 1), key, "second", "bob")
                ),
                withSource.values().get(i)
            );
            assertEquals(List.of(key), keysOnly.values().get(i));
        }
        assertEquals(40L, withSource.source().candidateRowsFetched());
        assertEquals(withSource.source().candidateRowsFetched(), keysOnly.source().candidateRowsFetched());
        assertEquals(withSource.source().candidateRowsFetched(), countOnly.source().candidateRowsFetched());
        assertEquals(10L, withSource.source().payloadRowsFetched());
        // Key-only output can replay selected key values; these counters measure all payload hits, not just source reads.
        assertEquals(0L, countOnly.source().payloadSearchRequests());
        assertEquals(0L, countOnly.source().payloadRowsFetched());
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
        try (var response = run(syncEsqlQueryRequest(SEQUENCE + " | KEEP key, stage1._source, stage2._source | LIMIT 1"))) {
            assertEquals(List.of(List.of("shared", reconstructed, second)), getValuesList(response));
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
        try (
            var response = run(syncEsqlQueryRequest(SEQUENCE + " | KEEP stage1._id, stage1._source, stage2._id, stage2._source | LIMIT 1"))
        ) {
            assertEquals(List.of(Arrays.asList("A", null, "B", second)), getValuesList(response));
        }
    }

    private Measured measure(String scenario, String query) {
        try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
            assertNotNull(response.profile());
            List<SequenceSourceStatus> sources = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            assertEquals("one native source status must survive query profiling", 1, sources.size());
            SequenceSourceStatus status = sources.getFirst();
            logger.debug("sequence_stage_source scenario={} native_status={}", scenario, status);
            return new Measured(getValuesList(response), status);
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

    /** Checks that the experiment releases its point-in-time contexts and ES|QL blocks after each test. */
    @After
    public void checkSequenceResourcesReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

    private record Measured(List<List<Object>> values, SequenceSourceStatus source) {}
}
