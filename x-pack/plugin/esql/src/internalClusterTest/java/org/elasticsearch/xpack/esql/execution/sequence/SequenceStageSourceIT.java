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
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.SequenceMatchDescription;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Tests ordinary primary source columns and original members returned through the retained-match contract. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceStageSourceIT extends AbstractSequenceIntegTestCase {
    private static final String FIRST = "sequence-stage-source-a";
    private static final String SECOND = "sequence-stage-source-b";
    private static final String THIRD = "sequence-stage-source-c";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final String SEQUENCE = """
        SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
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
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM sequence-stage-source-a METADATA _id, _index, _source
             | EVAL process.name = "computed-openssl" | RENAME payload AS renamed | DROP user.name
             | KEEP key, process.name, renamed, _id, _index, _source),
            (FROM sequence-stage-source-b METADATA _id, _index, _source
             | EVAL process.name = "computed-archiver" | KEEP key, process.name, _id, _index, _source)
            | EVAL member2_process_name = MEMBER(2, process.name)
            | WHERE member2_process_name == "computed-archiver"
            | KEEP key, process.name, member2_process_name
            | RETURN MATCHES
            """;
        try (var response = run(syncEsqlQueryRequest(query))) {
            var rows = retainedRows(response, SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT);
            assertEquals(1, rows.size());
            assertEquals(List.of("shared", "computed-openssl", "computed-archiver"), rows.getFirst().ordinary());
            assertEquals(
                List.of(
                    new SequenceMatchDescription.Member(1, FIRST, "A", first),
                    new SequenceMatchDescription.Member(2, SECOND, "B", second)
                ),
                rows.getFirst().members()
            );
        }
    }

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
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 3
            (FROM sequence-stage-source-a METADATA _id, _index, _source | KEEP key, _id, _index, _source),
            (FROM sequence-stage-source-b METADATA _id, _index, _source | KEEP key, _id, _index, _source),
            (FROM sequence-stage-source-c METADATA _id, _index, _source | KEEP key, _id, _index, _source)
            | RENAME _source AS primary_source, _index AS primary_index
            | KEEP key, primary_index, primary_source
            | RETURN MATCHES
            """;
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertEquals(3, SequenceMatchDescription.fromColumns(response.columns()).primaryStage());
            var rows = retainedRows(response, SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT);
            assertEquals(1, rows.size());
            assertEquals(List.of("shared", THIRD, third), rows.getFirst().ordinary());
            assertEquals(
                List.of(
                    new SequenceMatchDescription.Member(1, FIRST, "same-id", first),
                    new SequenceMatchDescription.Member(2, SECOND, "same-id", second),
                    new SequenceMatchDescription.Member(3, THIRD, "same-id", third)
                ),
                rows.getFirst().members()
            );
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

        Measured withSource = measure("with_source", SEQUENCE + " | KEEP key | SORT key | LIMIT 100 | RETURN MATCHES");
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
        try (var response = run(syncEsqlQueryRequest(SEQUENCE + " | KEEP key | LIMIT 1 | RETURN MATCHES"))) {
            assertEquals(
                List.of(List.of("shared", reconstructed, second)),
                retainedSourceValues(response, SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT)
            );
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
        try (var response = run(syncEsqlQueryRequest(SEQUENCE + " | KEEP key | LIMIT 1 | RETURN MATCHES"))) {
            var rows = retainedRows(response, SequenceMatchDescription.SourcePolicy.ALLOW_NULL);
            assertEquals(1, rows.size());
            assertEquals(List.of("shared"), rows.getFirst().ordinary());
            assertEquals(
                List.of(
                    new SequenceMatchDescription.Member(1, FIRST, "A", null),
                    new SequenceMatchDescription.Member(2, SECOND, "B", second)
                ),
                rows.getFirst().members()
            );
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
                SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY %s
                (FROM sequence-stage-source-a METADATA _source | MV_EXPAND key | KEEP key, _source),
                (FROM sequence-stage-source-b METADATA _source | MV_EXPAND key | KEEP key, _source)
                | KEEP key, _source | LIMIT 1
                """.formatted(primary))) {
                assertEquals(List.of(List.of("shared", primary == 1 ? first : second)), getValuesList(response));
            }
        }
    }

    private static List<SequenceMatchDescription.Row> retainedRows(
        EsqlQueryResponse response,
        SequenceMatchDescription.SourcePolicy sourcePolicy
    ) {
        var description = SequenceMatchDescription.fromColumns(response.columns());
        return getValuesList(response).stream().map(row -> description.decodeRow(row, sourcePolicy)).toList();
    }

    private static List<List<Object>> retainedSourceValues(EsqlQueryResponse response, SequenceMatchDescription.SourcePolicy sourcePolicy) {
        return retainedRows(response, sourcePolicy).stream().map(row -> {
            List<Object> values = new ArrayList<>(row.ordinary());
            row.members().forEach(member -> values.add(member.source()));
            return values;
        }).toList();
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
            return new Measured(
                query.endsWith("RETURN MATCHES")
                    ? retainedSourceValues(response, SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT)
                    : getValuesList(response),
                status
            );
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

    private record Measured(List<List<Object>> values, SequenceSourceStatus source) {}
}
