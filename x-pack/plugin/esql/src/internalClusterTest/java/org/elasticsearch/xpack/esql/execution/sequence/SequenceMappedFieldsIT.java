/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.MockDeterministicScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Differential coverage of mapped field storage layouts using ordinary ES|QL as the value oracle. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceMappedFieldsIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(
                CollectionUtils.appendToCopy(super.nodePlugins(), UnsignedLongMapperPlugin.class),
                FailingRuntimeFieldPlugin.class
            ),
            VersionFieldPlugin.class
        );
    }

    /** Real encoded version values match ordinary fields when selected rows load late output columns. */
    public void testVersionMappedValuesMatchOrdinaryAcrossStoragePreferencesAndPrimaryStages() {
        String index = "sequence-version-values";
        Map<String, Object> properties = baseProperties();
        properties.put("key", Map.of("type", "keyword", "store", true));
        properties.put("phase", Map.of("type", "keyword", "store", true));
        List<String> versions = List.of("1.2.3", "2.0.0-beta.1+build.7", "not a version");
        List<String> fields = List.of("version_valid", "version_prerelease", "version_invalid", "version_mv", "version_missing");
        for (String field : fields) {
            properties.put(field, Map.of("type", "version"));
        }
        createIndex(index, Map.of("_source", Map.of("enabled", false), "properties", properties), Settings.EMPTY);
        indexPair(
            index,
            Map.of(
                "version_valid",
                versions.get(0),
                "version_prerelease",
                versions.get(1),
                "version_invalid",
                versions.get(2),
                "version_mv",
                versions
            )
        );
        String projection = "key, phase, "
            + String.join(", ", fields)
            + ", other_phase, other_valid, other_prerelease, other_invalid, other_mv, other_missing";
        for (var preference : List.of(MappedFieldType.FieldExtractPreference.NONE, MappedFieldType.FieldExtractPreference.STORED)) {
            var pragmas = new QueryPragmas(Settings.builder().put("field_extract_preference", preference).build());
            for (int primary = 1; primary <= 2; primary++) {
                String phase = primary == 1 ? "A" : "B";
                String otherPhase = primary == 1 ? "B" : "A";
                String ordinary = Strings.format("""
                    FROM %s | WHERE phase=="%s"
                    | EVAL other_phase="%s", other_valid=version_valid, other_prerelease=version_prerelease,
                      other_invalid=version_invalid, other_mv=version_mv, other_missing=version_missing
                    | KEEP %s | LIMIT 1
                    """, index, phase, otherPhase, projection);
                // Fields used only in output load after matching without replaying the stage program.
                String sequence = Strings.format(
                    """
                        SEQUENCE BY key WITHIN 30s PRIMARY %s
                        (FROM %s | WHERE phase=="A" | KEEP key,phase,%s),
                        (FROM %s | WHERE phase=="B" | KEEP key,phase,%s)
                        | EVAL other_phase=MEMBER(%s,phase), other_valid=MEMBER(%s,version_valid),
                          other_prerelease=MEMBER(%s,version_prerelease), other_invalid=MEMBER(%s,version_invalid),
                          other_mv=MEMBER(%s,version_mv), other_missing=MEMBER(%s,version_missing)
                        | KEEP %s | LIMIT 1
                        """,
                    primary,
                    index,
                    String.join(",", fields),
                    index,
                    String.join(",", fields),
                    3 - primary,
                    3 - primary,
                    3 - primary,
                    3 - primary,
                    3 - primary,
                    3 - primary,
                    projection
                );
                try (
                    var ordinaryResponse = run(syncEsqlQueryRequest(ordinary).pragmas(pragmas).allowPartialResults(false));
                    var sequenceResponse = run(syncEsqlQueryRequest(sequence).pragmas(pragmas).allowPartialResults(false).profile(true))
                ) {
                    var expected = getValuesList(ordinaryResponse);
                    assertEquals(1, expected.size());
                    var row = expected.getFirst();
                    assertEquals(List.of("shared", phase, versions.get(0), versions.get(1), versions.get(2)), row.subList(0, 5));
                    assertTrue(row.get(5) instanceof List<?>);
                    assertEquals(3, ((List<?>) row.get(5)).size());
                    assertEquals(Set.copyOf(versions), Set.copyOf((List<?>) row.get(5)));
                    assertNull(row.get(6));
                    assertEquals(otherPhase, row.get(7));
                    assertEquals(row.subList(2, 7), row.subList(8, 13));
                    assertEquals(expected, getValuesList(sequenceResponse));
                    assertEquals(
                        ordinaryResponse.columns().stream().map(column -> List.of(column.name(), column.outputType())).toList(),
                        sequenceResponse.columns().stream().map(column -> List.of(column.name(), column.outputType())).toList()
                    );
                    assertEquals(
                        List.of(
                            "keyword",
                            "keyword",
                            "version",
                            "version",
                            "version",
                            "version",
                            "version",
                            "keyword",
                            "version",
                            "version",
                            "version",
                            "version",
                            "version"
                        ),
                        sequenceResponse.columns().stream().map(column -> column.outputType()).toList()
                    );
                    assertFalse(sequenceResponse.isPartial());
                    var statuses = sequenceResponse.profile()
                        .drivers()
                        .stream()
                        .flatMap(driver -> driver.operators().stream())
                        .map(OperatorStatus::status)
                        .filter(SequenceSourceStatus.class::isInstance)
                        .map(SequenceSourceStatus.class::cast)
                        .toList();
                    assertEquals(1, statuses.size());
                    SequenceSourceStatus status = statuses.getFirst();
                    assertEquals(2L, status.candidateSearchRequests());
                    assertEquals(2L, status.candidateRowsFetched());
                    assertEquals(1L, status.completedMatches());
                    assertEquals(1L, status.rowsEmitted());
                    assertEquals(2L, status.stageDrivers());
                    logger.info("SEQUENCE real version mapper preference={} primary={} profile={}", preference, primary, status);
                }
            }
        }
    }

    public void testSourceDisabledStoredKeywordAndTextLayouts() {
        Map<String, Object> properties = baseProperties();
        properties.put("key", Map.of("type", "keyword", "doc_values", false, "store", true));
        properties.put("phase", Map.of("type", "keyword", "store", true));
        properties.put("stored_keyword", Map.of("type", "keyword", "doc_values", false, "store", true));
        properties.put("text_own_values", Map.of("type", "text", "doc_values", true));
        properties.put("text_delegate", Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword"))));
        properties.put("text_stored", Map.of("type", "text", "store", true));
        properties.put("text_fielddata", Map.of("type", "text", "fielddata", true));
        properties.put("signed", Map.of("type", "long"));
        properties.put("unsigned", Map.of("type", "unsigned_long"));
        properties.put("ip", Map.of("type", "ip"));
        properties.put("millis", Map.of("type", "date"));
        createIndex("sequence-disabled", Map.of("_source", Map.of("enabled", false), "properties", properties), Settings.EMPTY);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("stored_keyword", List.of("second", "first", "second"));
        values.put("text_own_values", List.of("Complete Phrase", "Another Phrase", "Complete Phrase"));
        values.put("text_delegate", "A Full Phrase With Spaces");
        values.put("text_stored", List.of("Stored Whole Phrase", "Stored Other Phrase"));
        values.put("text_fielddata", "Analyzed Tokens Must Stay Absent");
        values.put("signed", 9_007_199_254_740_993L);
        values.put("unsigned", "18446744073709551615");
        values.put("ip", "2001:db8::1");
        values.put("millis", BASE.toString());
        indexPair("sequence-disabled", values);
        List<String> columns = new ArrayList<>(values.keySet());
        columns.addFirst("key");
        assertOrdinaryParity("sequence-disabled", columns, "");
        // The key itself is stored without doc values; both stages must read it to complete the sequence.
        // Fielddata on a source-disabled text column must not substitute analyzed tokens for the missing original text.
        try (var response = run("from sequence-disabled | where phase==\"A\" | keep text_fielddata | limit 1")) {
            assertNull(getValuesList(response).getFirst().getFirst());
        }
        try (var response = run(syncEsqlQueryRequest("""
            sequence by normalized within 30s PRIMARY 1
            (from sequence-disabled | where phase=="A" | eval normalized=TO_LOWER(text_delegate)),
            (from sequence-disabled | where phase=="B" | eval normalized=TO_LOWER(text_delegate))
            | keep normalized, text_delegate | limit 1
            """).pragmas(new QueryPragmas(Settings.EMPTY)))) {
            assertValues(response.values(), List.of(List.of("a full phrase with spaces", "A Full Phrase With Spaces")));
        }
    }

    public void testFlattenedRootReconstructionAndStageExtraction() {
        for (boolean sourceEnabled : List.of(true, false)) {
            String index = "sequence-flattened-" + sourceEnabled;
            var properties = baseProperties();
            properties.put("attributes", Map.of("type", "flattened"));
            if (sourceEnabled == false) {
                properties.put("key", Map.of("type", "keyword", "store", true));
                properties.put("phase", Map.of("type", "keyword", "store", true));
            }
            createIndex(index, Map.of("_source", Map.of("enabled", sourceEnabled), "properties", properties), Settings.EMPTY);
            indexPair(
                index,
                Map.of("attributes", Map.of("product", "Agent", "nested", Map.of("level", 7), "tags", List.of("z", "a", "z")))
            );
            assertOrdinaryParity(index, List.of("key", "attributes", "product"), "| eval product=FIELD_EXTRACT(attributes, \"product\")");
            try (var response = run("from " + index + " | where phase==\"A\" | keep attributes | limit 1")) {
                String value = (String) getValuesList(response).getFirst().getFirst();
                assertThat(value, containsString("product"));
                assertThat(value, containsString("nested"));
            }
        }
    }

    public void testColumnarArraysPreserveOrdinaryOrderDuplicatesAndNullHandling() {
        var properties = baseProperties();
        properties.put("labels", Map.of("type", "keyword"));
        properties.put("text", Map.of("type", "text", "doc_values", true));
        properties.put("numbers", Map.of("type", "long"));
        properties.put("attributes", Map.of("type", "flattened"));
        createIndex(
            "sequence-columnar",
            Map.of("properties", properties),
            Settings.builder().put("index.mode", IndexMode.COLUMNAR.getName()).build()
        );
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("labels", Arrays.asList("z", null, "a", "z"));
        values.put("text", Arrays.asList("Last Phrase", "First Phrase", null, "Last Phrase"));
        values.put("numbers", Arrays.asList(9_007_199_254_740_995L, null, 9_007_199_254_740_993L, 9_007_199_254_740_995L));
        values.put("attributes", Map.of("labels", Arrays.asList("z", null, "a", "z")));
        indexPair("sequence-columnar", values);
        assertOrdinaryParity("sequence-columnar", List.of("key", "labels", "text", "numbers", "attributes"), "");
        // Pin a visible difference from sorted/deduplicated fielddata so the oracle comparison cannot pass vacuously.
        try (var response = run("from sequence-columnar | where phase==\"A\" | keep labels | limit 1")) {
            assertEquals(List.of("z", "a", "z"), getValuesList(response).getFirst().getFirst());
        }
        assertOrdinaryParity(
            "sequence-columnar",
            List.of("key", "label_count", "first_label", "last_number"),
            "| eval label_count=MV_COUNT(labels), first_label=MV_FIRST(labels), last_number=MV_LAST(numbers)"
        );
    }

    public void testMappedLoadingFailureReleasesReadersAndSnapshot() {
        createIndex(
            "sequence-loading-failure",
            Map.of(
                "properties",
                baseProperties(),
                "runtime",
                Map.of("failure", Map.of("type", "long", "script", Map.of("lang", "sequence_failure", "source", "failure")))
            ),
            Settings.EMPTY
        );
        indexPair("sequence-loading-failure", Map.of());
        RuntimeException failure = expectThrows(RuntimeException.class, () -> run(syncEsqlQueryRequest("""
            sequence by key within 30s PRIMARY 1
            (from sequence-loading-failure | where phase=="A"),
            (from sequence-loading-failure | where phase=="B")
            | keep failure | limit 1
            """).allowPartialResults(false)));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("sequence mapped loading failure"));
    }

    /** Runtime fields use the ordinary mapped loader and their demanded outputs survive first-read capture. */
    public void testDeterministicRuntimeKeyAndCapturedOutput() {
        createRuntimeIndex("sequence-repeatable", "constant");
        try (var response = run("""
            sequence by changing within 30s PRIMARY 1
            (from sequence-repeatable | where phase=="A" | eval payload=changing+1),
            (from sequence-repeatable | where phase=="B" | eval payload=changing+1)
            | EVAL member2_payload = MEMBER(2, payload)
            | keep changing, phase, member2_payload | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of(1L, "A", 2L)));
        }
    }

    /** First-read capture accepts runtime values that can change between independent searches. */
    public void testNondeterministicRuntimeKeyAndPredicateAreAccepted() {
        createRuntimeIndex("sequence-nondeterministic", "changing");
        for (String query : List.of("""
            SEQUENCE BY changing WITHIN 30s PRIMARY 1
            (FROM sequence-nondeterministic | WHERE phase=="A"), (FROM sequence-nondeterministic | WHERE phase=="B")
            | KEEP changing | LIMIT 1
            """, """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-nondeterministic | WHERE phase=="A" AND changing<0),
            (FROM sequence-nondeterministic | WHERE phase=="B") | KEEP key | LIMIT 1
            """)) {
            try (var response = run(query)) {
                assertValues(response.values(), List.of());
            }
            assertTrue(FailingRuntimeFieldPlugin.EXECUTIONS.get() > 0);
        }
    }

    /** Source time still requires indexed values when the output projection hides the timestamp. */
    public void testNondeterministicRuntimeTimestampIsRefusedWhenOutputPruned() {
        FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        createIndex(
            "sequence-runtime-time",
            Map.of("properties", baseProperties(), "runtime", Map.of("@timestamp", runtimeField("date", "changing"))),
            Settings.EMPTY
        );
        indexPair("sequence-runtime-time", Map.of());
        Exception failure = expectThrows(Exception.class, () -> {
            try (var response = run("""
                SEQUENCE BY key WITHIN 30s PRIMARY 1
                (FROM sequence-runtime-time | WHERE phase=="A"), (FROM sequence-runtime-time | WHERE phase=="B")
                | KEEP key | LIMIT 1
                """)) {
                getValuesList(response);
            }
        });
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("SEQUENCE does not support a runtime [@timestamp]"));
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    /** Output demands are loaded with source hits even when the query cannot complete a match. */
    public void testNondeterministicOutputIsLoadedEvenWithoutMatch() {
        createRuntimeIndex("sequence-output-runtime", "changing");
        try (var response = run("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-output-runtime | WHERE phase=="A"), (FROM sequence-output-runtime | WHERE phase=="missing")
            | KEEP changing | LIMIT 1
            """)) {
            assertValues(response.values(), List.of());
        }
        assertTrue(FailingRuntimeFieldPlugin.EXECUTIONS.get() > 0);
    }

    /** An irrelevant later source is not globally linted after compute rejects every possible start. */
    public void testNondeterministicLaterDemandIsUnvisitedWithoutEligibleStart() {
        String query = createNondeterministicLaterDemandQuery(0);
        try (var response = run(syncEsqlQueryRequest(query).allowPartialResults(false))) {
            assertValues(response.values(), List.of());
        }
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    /** An eligible start causes the later stage to load and retain its demanded runtime value. */
    public void testNondeterministicLaterDemandIsCapturedWithEligibleStart() {
        String query = createNondeterministicLaterDemandQuery(2);
        try (var response = run(query)) {
            List<List<Object>> values = getValuesList(response);
            assertEquals(1, values.size());
            assertEquals("shared", values.getFirst().getFirst());
            assertTrue(((Number) values.getFirst().get(1)).longValue() > 0);
        }
        assertTrue(FailingRuntimeFieldPlugin.EXECUTIONS.get() > 0);
    }

    private String createNondeterministicLaterDemandQuery(int accepted) {
        var properties = baseProperties();
        properties.put("payload", Map.of("type", "keyword"));
        createIndex("sequence-runtime-first", Map.of("properties", properties), Settings.EMPTY);
        indexPair("sequence-runtime-first", Map.of("payload", "1"));
        createRuntimeIndex("sequence-runtime-later", "changing");
        return Strings.format("""
            sequence by key within 30s PRIMARY 1
            (from sequence-runtime-first | where phase=="A"
                | eval eligible=LENGTH(phase)+TO_LONG(payload) | where eligible==%s),
            (from sequence-runtime-later | where phase=="B")
            | EVAL later_value = MEMBER(2, changing)
            | keep key, later_value | limit 1
            """, accepted);
    }

    /** Unsupported unused runtime fields are pruned, and ordinary ES|QL keeps its existing evaluation semantics. */
    public void testUnusedNondeterministicRuntimeIsPrunedAndOrdinaryQueryIsUnchanged() {
        createRuntimeIndex("sequence-unused-runtime", "changing");
        try (var response = run("""
            sequence by key within 30s PRIMARY 1
            (from sequence-unused-runtime | where phase=="A"), (from sequence-unused-runtime | where phase=="B")
            | keep key | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared")));
        }
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
        try (var response = run("from sequence-unused-runtime | keep changing | limit 1")) {
            assertEquals(1, getValuesList(response).size());
        }
        assertTrue(FailingRuntimeFieldPlugin.EXECUTIONS.get() > 0);
    }

    /** A runtime dependency follows ordinary mapped loading even when its value can change across reads. */
    public void testRuntimeDependencyIsAcceptedWithFirstReadCapture() {
        FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        createIndex(
            "sequence-runtime-dependency",
            Map.of(
                "properties",
                baseProperties(),
                "runtime",
                Map.of("changing", runtimeField("long", "changing"), "dependent", runtimeField("long", "dependent"))
            ),
            Settings.EMPTY
        );
        indexPair("sequence-runtime-dependency", Map.of());
        try (var response = run("""
            SEQUENCE BY dependent WITHIN 30s PRIMARY 1
            (FROM sequence-runtime-dependency | WHERE phase=="A"), (FROM sequence-runtime-dependency | WHERE phase=="B")
            | KEEP dependent | LIMIT 1
            """)) {
            assertValues(response.values(), List.of());
        }
        assertTrue(FailingRuntimeFieldPlugin.EXECUTIONS.get() > 0);
    }

    /** Request-filter scripts follow ordinary search semantics and the fixed NOW/date-math clock remains usable. */
    public void testNondeterministicRequestFilterAndFixedNowAreAccepted() {
        createRuntimeIndex("sequence-filter-runtime", "constant");
        String query = """
            sequence by key within 30s PRIMARY 1
            (from sequence-filter-runtime | where phase=="A"), (from sequence-filter-runtime | where phase=="B")
            | keep key | limit 1
            """;
        try (
            var response = run(
                syncEsqlQueryRequest(query).filter(
                    QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, "sequence_failure", "filter", Map.of()))
                ).allowPartialResults(false)
            )
        ) {
            assertValues(response.values(), List.of());
        }
        try (
            var response = run(
                syncEsqlQueryRequest(query).filter(QueryBuilders.rangeQuery("@timestamp").gte("now-10y").lte("now+10y"))
                    .allowPartialResults(false)
            )
        ) {
            assertValues(response.values(), List.of(List.of("shared")));
        }
    }

    /** Ordinary asynchronous terms lookup is accepted after removing the second-read repeatability guard. */
    public void testTermsLookupRequestFilterMatchesNativeSearch() {
        createRuntimeIndex("sequence-lookup-source", "constant");
        for (String phase : List.of("A", "B")) {
            prepareIndex("sequence-lookup-source").setId("other-" + phase)
                .setSource("@timestamp", (phase.equals("A") ? BASE : BASE.plusNanos(1)).toString(), "key", "other", "phase", phase)
                .get();
        }
        refresh("sequence-lookup-source");
        createIndex("sequence-lookup-values", Map.of("properties", Map.of("keys", Map.of("type", "keyword"))), Settings.EMPTY);
        prepareIndex("sequence-lookup-values").setId("allowed").setSource(Map.of("keys", List.of("shared"))).get();
        refresh("sequence-lookup-values");
        var filter = new org.elasticsearch.index.query.TermsQueryBuilder(
            "key",
            new org.elasticsearch.indices.TermsLookup("sequence-lookup-values", "allowed", "keys")
        );
        // This snapshot's ordinary ES|QL filter planner does not execute asynchronous terms lookup. Native search
        // is the control: SEQUENCE uses that same search machinery, and must exclude the second key's events.
        SearchResponse control = client().prepareSearch("sequence-lookup-source")
            .setQuery(filter)
            .setTrackTotalHits(true)
            .setSize(10)
            .get();
        try {
            assertEquals(2L, control.getHits().getTotalHits().value());
        } finally {
            control.decRef();
        }
        try (var response = run(syncEsqlQueryRequest("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-lookup-source | WHERE phase=="A"), (FROM sequence-lookup-source | WHERE phase=="B")
            | KEEP key | LIMIT 10
            """).filter(filter).allowPartialResults(false))) {
            assertValues(response.values(), List.of(List.of("shared")));
        }
    }

    private void createRuntimeIndex(String index, String script) {
        FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        createIndex(
            index,
            Map.of("properties", baseProperties(), "runtime", Map.of("changing", runtimeField("long", script))),
            Settings.EMPTY
        );
        indexPair(index, Map.of());
    }

    /** A PIT preserves its documents while later reads use the live runtime definition, including zero-hit reads. */
    public void testRuntimeDefinitionChangeUsesLiveDefinitionOnOldPitIncludingZeroHitRead() {
        String index = "sequence-runtime-epoch";
        createRuntimeIndex(index, "constant");
        BytesReference pit = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).keepAlive(TimeValue.timeValueMinutes(2))
        ).actionGet().getPointInTimeId();
        try {
            SearchResponse first = guardedPitRead(pit, index, "A");
            try {
                assertEquals(1L, first.getHits().getTotalHits().value());
                assertEquals(List.of(1L), first.getHits().getAt(0).field("changing").getValues());
                if (first.pointInTimeId() != null) pit = first.pointInTimeId();
            } finally {
                first.decRef();
            }
            assertAcked(
                indicesAdmin().preparePutMapping(index).setSource(Map.of("runtime", Map.of("changing", runtimeField("long", "two"))))
            );
            for (String phase : List.of("B", "missing")) {
                SearchResponse live = guardedPitRead(pit, index, phase);
                try {
                    assertEquals(phase.equals("B") ? 1L : 0L, live.getHits().getTotalHits().value());
                    if (phase.equals("B")) {
                        assertEquals(List.of(2L), live.getHits().getAt(0).field("changing").getValues());
                    }
                    if (live.pointInTimeId() != null) pit = live.pointInTimeId();
                } finally {
                    live.decRef();
                }
            }
            try (var ordinary = run("from " + index + " | keep changing | limit 1")) {
                assertValues(ordinary.values(), List.of(List.of(2L)));
            }
            try (var fresh = run("""
                sequence by changing within 30s PRIMARY 1
                (from sequence-runtime-epoch | where phase=="A"), (from sequence-runtime-epoch | where phase=="B")
                | keep changing | limit 1
                """)) {
                assertValues(fresh.values(), List.of(List.of(2L)));
            }
        } finally {
            assertTrue(client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded());
        }
    }

    /** Adding an unused dynamic field does not reject an existing PIT or make newly indexed documents visible to it. */
    public void testUnrelatedDynamicFieldAdditionPreservesOldPit() throws Exception {
        String index = "sequence-unrelated-dynamic-field";
        String unrelatedField = "unrelated_dynamic_field";
        createRuntimeIndex(index, "constant");
        assertNull(client().prepareFieldCaps(index).setFields(unrelatedField).get().getField(unrelatedField));
        BytesReference pit = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).keepAlive(TimeValue.timeValueMinutes(2))
        ).actionGet().getPointInTimeId();
        try {
            SearchResponse first = guardedPitRead(pit, index, "A");
            try {
                assertEquals(1L, first.getHits().getTotalHits().value());
                if (first.pointInTimeId() != null) pit = first.pointInTimeId();
            } finally {
                first.decRef();
            }

            // The guarded read demands "changing"; neither it nor the fresh query requests this new field.
            prepareIndex(index).setId("unrelated")
                .setSource(Map.of("@timestamp", BASE.plusNanos(2).toString(), "key", "unrelated", "phase", "unrelated", unrelatedField, 1))
                .get();
            assertBusy(() -> assertNotNull(client().prepareFieldCaps(index).setFields(unrelatedField).get().getField(unrelatedField)));
            refresh(index);
            for (String phase : List.of("B", "missing", "unrelated")) {
                SearchResponse live = guardedPitRead(pit, index, phase);
                try {
                    assertEquals(phase.equals("B") ? 1L : 0L, live.getHits().getTotalHits().value());
                    if (phase.equals("B")) {
                        assertEquals(List.of(1L), live.getHits().getAt(0).field("changing").getValues());
                    }
                    if (live.pointInTimeId() != null) pit = live.pointInTimeId();
                } finally {
                    live.decRef();
                }
            }
            try (var fresh = run("""
                sequence by changing within 30s PRIMARY 1
                (from sequence-unrelated-dynamic-field | where phase=="A"),
                (from sequence-unrelated-dynamic-field | where phase=="B")
                | keep changing | limit 1
                """)) {
                assertValues(fresh.values(), List.of(List.of(1L)));
            }
        } finally {
            assertTrue(client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded());
        }
    }

    private SearchResponse guardedPitRead(BytesReference pit, String index, String phase) {
        return client().prepareSearch()
            .setSource(
                new SearchSourceBuilder().query(new SequenceSourceQueryBuilder(QueryBuilders.termQuery("phase", phase), List.of(index)))
                    .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2)))
                    .fetchField("changing")
                    .size(1)
                    .trackTotalHits(true)
            )
            .setPreference(null)
            .setPreFilterShardSize(Integer.MAX_VALUE)
            .setAllowPartialSearchResults(false)
            .get();
    }

    private static Map<String, Object> runtimeField(String type, String script) {
        return Map.of("type", type, "script", Map.of("lang", "sequence_failure", "source", script));
    }

    /**
     * Injects a deterministic script failure while keeping the real runtime mapper, block loader and fetch path.
     * The existing integration-test script engine avoids a dependency on the full Painless module for this failure fixture.
     */
    public static class FailingRuntimeFieldPlugin extends Plugin implements ScriptPlugin {
        static final AtomicInteger EXECUTIONS = new AtomicInteger();

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            Map<String, Function<Map<String, Object>, Object>> deterministic = Map.of("failure", vars -> {
                throw new IllegalArgumentException("sequence mapped loading failure");
            },
                "constant",
                vars -> 1L,
                "two",
                vars -> 2L,
                "timestamp",
                vars -> ((Number) vars.get("millis")).longValue(),
                "dependent",
                vars -> ((org.elasticsearch.index.fielddata.ScriptDocValues<?>) ((Map<?, ?>) vars.get("doc")).get("changing")).get(0)
            );
            Map<String, Function<Map<String, Object>, Object>> nondeterministic = Map.of(
                "changing",
                vars -> (long) EXECUTIONS.incrementAndGet(),
                "filter",
                vars -> false
            );
            Map<ScriptContext<?>, MockScriptEngine.ContextCompiler> runtimeCompilers = Map.of(
                LongFieldScript.CONTEXT,
                (script, compilerParams) -> new LongFieldScript.Factory() {
                    @Override
                    public boolean isResultDeterministic() {
                        return script instanceof ScriptFactory factory && factory.isResultDeterministic();
                    }

                    @Override
                    public LongFieldScript.LeafFactory newFactory(
                        String fieldName,
                        Map<String, Object> params,
                        org.elasticsearch.search.lookup.SearchLookup lookup,
                        org.elasticsearch.index.mapper.OnScriptError onScriptError
                    ) {
                        return leaf -> new LongFieldScript(fieldName, params, lookup, onScriptError, leaf) {
                            @Override
                            public void execute() {
                                var vars = new HashMap<String, Object>(params);
                                vars.put("doc", getDoc());
                                emit(((Number) script.apply(vars)).longValue());
                            }
                        };
                    }
                },
                DateFieldScript.CONTEXT,
                (script, compilerParams) -> new DateFieldScript.Factory() {
                    @Override
                    public boolean isResultDeterministic() {
                        return script instanceof ScriptFactory factory && factory.isResultDeterministic();
                    }

                    @Override
                    public DateFieldScript.LeafFactory newFactory(
                        String fieldName,
                        Map<String, Object> params,
                        org.elasticsearch.search.lookup.SearchLookup lookup,
                        org.elasticsearch.common.time.DateFormatter formatter,
                        org.elasticsearch.index.mapper.OnScriptError onScriptError
                    ) {
                        return leaf -> new DateFieldScript(fieldName, params, lookup, formatter, onScriptError, leaf) {
                            @Override
                            public void execute() {
                                emit(((Number) script.apply(params)).longValue());
                            }
                        };
                    }
                }
            );
            return new MockScriptEngine("sequence_failure", deterministic, nondeterministic, runtimeCompilers) {
                @Override
                public <T> T compile(String name, String source, ScriptContext<T> context, Map<String, String> params) {
                    // MockScriptEngine handles runtime contexts before its custom compiler map. Intercept those
                    // contexts so the declared function and its actual determinism flag both reach the real mapper.
                    var runtimeCompiler = runtimeCompilers.get(context);
                    if (runtimeCompiler == null) {
                        return super.compile(name, source, context, params);
                    }
                    boolean repeatable = deterministic.containsKey(source);
                    Function<Map<String, Object>, Object> function = repeatable ? deterministic.get(source) : nondeterministic.get(source);
                    if (function == null) {
                        return super.compile(name, source, context, params);
                    }
                    MockDeterministicScript script = repeatable
                        ? MockDeterministicScript.asDeterministic(function)
                        : MockDeterministicScript.asNonDeterministic(function);
                    return context.factoryClazz.cast(runtimeCompiler.compile(script, params));
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT, DateFieldScript.CONTEXT, FilterScript.CONTEXT);
                }
            };
        }
    }

    public void testMixedDateMappingRequiresConversionButUnusedPayloadIsPruned() {
        for (String type : List.of("date", "date_nanos")) {
            var properties = baseProperties();
            properties.put("payload_time", Map.of("type", type));
            String index = "sequence-mixed-" + type;
            createIndex(index, Map.of("properties", properties), Settings.EMPTY);
            indexPair(index, Map.of("payload_time", BASE.toString()));
        }
        var pragmas = new QueryPragmas(Settings.EMPTY);
        try (var ordinary = run(syncEsqlQueryRequest("""
            from sequence-mixed-* | where phase=="A"
            | keep payload_time | sort payload_time | limit 10
            """).pragmas(pragmas))) {
            assertEquals(
                List.of(Instant.parse("2026-09-25T21:14:51.389Z"), BASE),
                getValuesList(ordinary).stream().map(row -> Instant.parse((String) row.getFirst())).toList()
            );
        }
        String sequence = """
            sequence by key within 30s PRIMARY 1
            (from sequence-mixed-* | where phase=="A"),
            (from sequence-mixed-* | where phase=="B")
            """;
        var failure = expectThrows(
            VerificationException.class,
            () -> run(syncEsqlQueryRequest(sequence + " | keep payload_time | limit 1").pragmas(pragmas))
        );
        assertThat(failure.getMessage(), containsString("source column [payload_time] requiring a conversion across mappings"));
        try (var pruned = run(syncEsqlQueryRequest(sequence + " | keep key | limit 1").pragmas(pragmas))) {
            assertValues(pruned.values(), List.of(List.of("shared")));
        }
    }

    private void assertOrdinaryParity(String index, List<String> columns, String derivation) {
        for (var preference : List.of(MappedFieldType.FieldExtractPreference.NONE, MappedFieldType.FieldExtractPreference.STORED)) {
            var pragmas = new QueryPragmas(Settings.builder().put("field_extract_preference", preference).build());
            for (int stage = 1; stage <= 2; stage++) {
                String phase = stage == 1 ? "A" : "B";
                String ordinary = "from " + index + " | where phase==\"" + phase + "\" " + derivation;
                String sequence = Strings.format("""
                    sequence by key within 30s PRIMARY %s
                    (from %s | where phase=="A" %s), (from %s | where phase=="B" %s)
                    """, stage, index, derivation, index, derivation);
                String projection = String.join(", ", columns);
                try (
                    var ordinaryResponse = run(syncEsqlQueryRequest(ordinary + " | keep " + projection + " | limit 1").pragmas(pragmas));
                    var sequenceResponse = run(syncEsqlQueryRequest(sequence + " | keep " + projection + " | limit 1").pragmas(pragmas))
                ) {
                    assertEquals(getValuesList(ordinaryResponse), getValuesList(sequenceResponse));
                    assertEquals(
                        ordinaryResponse.columns().stream().map(column -> column.type()).toList(),
                        sequenceResponse.columns().stream().map(column -> column.type()).toList()
                    );
                    assertEquals(1, getValuesList(sequenceResponse).size());
                }
            }
        }
    }

    private static Map<String, Object> baseProperties() {
        var properties = new LinkedHashMap<String, Object>();
        properties.put("@timestamp", Map.of("type", "date_nanos"));
        properties.put("key", Map.of("type", "keyword"));
        properties.put("phase", Map.of("type", "keyword"));
        return properties;
    }

    private void createIndex(String index, Map<String, Object> mapping, Settings settings) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(settings)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.max_docvalue_fields_search", 0)
                )
                .setMapping(mapping)
        );
    }

    private void indexPair(String index, Map<String, Object> values) {
        var bulk = client().prepareBulk();
        for (String phase : List.of("A", "B")) {
            Map<String, Object> source = new LinkedHashMap<>(values);
            source.put("@timestamp", (phase.equals("A") ? BASE : BASE.plusNanos(1)).toString());
            source.put("key", "shared");
            source.put("phase", phase);
            bulk.add(prepareIndex(index).setId(phase).setSource(source));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
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
