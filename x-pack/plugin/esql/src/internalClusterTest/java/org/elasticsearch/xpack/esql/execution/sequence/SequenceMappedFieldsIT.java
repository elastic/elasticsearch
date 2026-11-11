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
            CollectionUtils.appendToCopy(super.nodePlugins(), UnsignedLongMapperPlugin.class),
            FailingRuntimeFieldPlugin.class
        );
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
            sequence by normalized with {"maxspan":"30s"} PRIMARY 1
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
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from sequence-loading-failure | where phase=="A"),
            (from sequence-loading-failure | where phase=="B")
            | keep failure | limit 1
            """).allowPartialResults(false)));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("sequence mapped loading failure"));
    }

    /** Repeatable runtime fields retain the ordinary mapped-loader and selected-replay path. */
    public void testDeterministicRuntimeKeyAndPayloadReplay() {
        createRuntimeIndex("sequence-repeatable", "constant");
        try (var response = run("""
            sequence by changing with {"maxspan":"30s"} PRIMARY 1
            (from sequence-repeatable | where phase=="A" | eval payload=changing+1),
            (from sequence-repeatable | where phase=="B" | eval payload=changing+1)
            | EVAL member2_payload = MEMBER(2, payload)
            | keep changing, phase, member2_payload | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of(1L, "A", 2L)));
        }
    }

    /** A declared nondeterministic key or zero-hit predicate is rejected before any script value is evaluated. */
    public void testNondeterministicRuntimeKeyAndPredicateAreRejected() {
        createRuntimeIndex("sequence-nondeterministic", "changing");
        for (String query : List.of("""
            sequence by changing with {"maxspan":"30s"} PRIMARY 1
            (from sequence-nondeterministic | where phase=="A"), (from sequence-nondeterministic | where phase=="B")
            | keep changing | limit 1
            """, """
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from sequence-nondeterministic | where phase=="A" and changing<0),
            (from sequence-nondeterministic | where phase=="B")
            | keep key | limit 1
            """)) {
            assertNonRepeatable(query, "nondeterministic runtime field [changing]");
            assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
        }
    }

    /** Timestamp validation is mandatory even when the visible projection and matching keys do not request it. */
    public void testNondeterministicRuntimeTimestampRejectedWhenPruned() {
        FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        createIndex(
            "sequence-runtime-time",
            Map.of("properties", baseProperties(), "runtime", Map.of("@timestamp", runtimeField("date", "changing"))),
            Settings.EMPTY
        );
        indexPair("sequence-runtime-time", Map.of());
        assertNonRepeatable("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from sequence-runtime-time | where phase=="A"), (from sequence-runtime-time | where phase=="B")
            | keep key | limit 1
            """, "nondeterministic runtime field [@timestamp]");
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    /** Output-only demands are validated in candidate search, including a query that cannot complete a match. */
    public void testNondeterministicOutputOnlyRuntimeIsRejectedWithoutMatch() {
        createRuntimeIndex("sequence-output-runtime", "changing");
        assertNonRepeatable("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from sequence-output-runtime | where phase=="A"), (from sequence-output-runtime | where phase=="missing")
            | keep changing | limit 1
            """, "nondeterministic runtime field [changing]");
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    /** An irrelevant later source is not globally linted after compute rejects every possible start. */
    public void testNondeterministicLaterDemandIsUnvisitedWithoutEligibleStart() {
        String query = createNondeterministicLaterDemandQuery(0);
        try (var response = run(syncEsqlQueryRequest(query).allowPartialResults(false))) {
            assertValues(response.values(), List.of());
        }
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    /** Changing only the first-stage computed predicate makes the same later demand mandatory to validate. */
    public void testNondeterministicLaterDemandIsRejectedWithEligibleStart() {
        String query = createNondeterministicLaterDemandQuery(2);
        assertNonRepeatable(query, "nondeterministic runtime field [changing]");
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    private String createNondeterministicLaterDemandQuery(int accepted) {
        var properties = baseProperties();
        properties.put("payload", Map.of("type", "keyword"));
        createIndex("sequence-runtime-first", Map.of("properties", properties), Settings.EMPTY);
        indexPair("sequence-runtime-first", Map.of("payload", "1"));
        createRuntimeIndex("sequence-runtime-later", "changing");
        return Strings.format("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
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
            sequence by key with {"maxspan":"30s"} PRIMARY 1
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

    /** A deterministic script cannot hide a nondeterministic runtime dependency behind a document lookup. */
    public void testDeterministicRuntimeCannotHideNondeterministicDependency() {
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
        assertNonRepeatable("""
            sequence by dependent with {"maxspan":"30s"} PRIMARY 1
            (from sequence-runtime-dependency | where phase=="A"), (from sequence-runtime-dependency | where phase=="B")
            | keep dependent | limit 1
            """, "nondeterministic runtime field [changing]");
        assertEquals(0, FailingRuntimeFieldPlugin.EXECUTIONS.get());
    }

    /** Request-filter script factories must be repeatable, while the query's fixed NOW/date-math clock stays usable. */
    public void testNondeterministicRequestFilterIsRejectedAndFixedNowRemainsSupported() {
        createRuntimeIndex("sequence-filter-runtime", "constant");
        String query = """
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from sequence-filter-runtime | where phase=="A"), (from sequence-filter-runtime | where phase=="B")
            | keep key | limit 1
            """;
        RuntimeException failure = expectThrows(
            RuntimeException.class,
            () -> run(
                syncEsqlQueryRequest(query).filter(
                    QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, "sequence_failure", "filter", Map.of()))
                ).allowPartialResults(false)
            )
        );
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("requires repeatable evaluation"));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("nondeterministic script"));
        try (
            var response = run(
                syncEsqlQueryRequest(query).filter(QueryBuilders.rangeQuery("@timestamp").gte("now-10y").lte("now+10y"))
                    .allowPartialResults(false)
            )
        ) {
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

    /** A truthful pure runtime definition is still mutable; old PIT reads fail and fresh sequence reads use the new definition. */
    public void testRuntimeDefinitionChangeRejectsOldPitIncludingZeroHitRead() {
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
                if (first.pointInTimeId() != null) pit = first.pointInTimeId();
            } finally {
                first.decRef();
            }
            assertAcked(
                indicesAdmin().preparePutMapping(index).setSource(Map.of("runtime", Map.of("changing", runtimeField("long", "two"))))
            );
            BytesReference oldPit = pit;
            for (String phase : List.of("B", "missing")) {
                RuntimeException failure = expectThrows(RuntimeException.class, () -> {
                    SearchResponse unexpected = guardedPitRead(oldPit, index, phase);
                    unexpected.decRef();
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString("mapping changes since the PIT reader was opened"));
            }
            try (var ordinary = run("from " + index + " | keep changing | limit 1")) {
                assertValues(ordinary.values(), List.of(List.of(2L)));
            }
            try (var fresh = run("""
                sequence by changing with {"maxspan":"30s"} PRIMARY 1
                (from sequence-runtime-epoch | where phase=="A"), (from sequence-runtime-epoch | where phase=="B")
                | keep changing | limit 1
                """)) {
                assertValues(fresh.values(), List.of(List.of(2L)));
            }
        } finally {
            assertTrue(client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded());
        }
    }

    /** Even an unused dynamic field replaces the mapping identity; a fresh sequence must remain usable afterward. */
    public void testUnrelatedDynamicFieldAdditionRejectsOldPit() throws Exception {
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
            BytesReference oldPit = pit;
            for (String phase : List.of("B", "missing")) {
                RuntimeException failure = expectThrows(RuntimeException.class, () -> {
                    SearchResponse unexpected = guardedPitRead(oldPit, index, phase);
                    unexpected.decRef();
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString("mapping changes since the PIT reader was opened"));
            }
            try (var fresh = run("""
                sequence by changing with {"maxspan":"30s"} PRIMARY 1
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
                new SearchSourceBuilder().query(
                    new SequenceRepeatabilityQueryBuilder(QueryBuilders.termQuery("phase", phase), List.of("changing"), List.of(index))
                ).pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2))).size(1).trackTotalHits(true)
            )
            .setPreference(null)
            .setPreFilterShardSize(Integer.MAX_VALUE)
            .setAllowPartialSearchResults(false)
            .get();
    }

    private static Map<String, Object> runtimeField(String type, String script) {
        return Map.of("type", type, "script", Map.of("lang", "sequence_failure", "source", script));
    }

    private void assertNonRepeatable(String query, String feature) {
        RuntimeException failure = expectThrows(RuntimeException.class, () -> run(syncEsqlQueryRequest(query).allowPartialResults(false)));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("requires repeatable evaluation"));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString(feature));
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
            sequence by key with {"maxspan":"30s"} PRIMARY 1
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
                    sequence by key with {"maxspan":"30s"} PRIMARY %s
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
