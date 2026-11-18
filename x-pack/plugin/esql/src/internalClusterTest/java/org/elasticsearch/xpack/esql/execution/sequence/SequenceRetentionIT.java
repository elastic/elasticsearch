/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
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

/** First-read retention preserves typed values, source identities and bounded diagnostics. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceRetentionIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");
    private static final String FIRST = "sequence-retention-a";
    private static final String SECOND = "sequence-retention-b";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class);
    }

    public void testRetainedNullMultivaluePrecisionAndNanosMatchOrdinaryValues() {
        createIndex(FIRST, false);
        createIndex(SECOND, false);
        for (String index : List.of(FIRST, SECOND)) {
            Map<String, Object> event = source(index.equals(FIRST) ? 0 : 1, "shared", index.equals(FIRST) ? "A" : "B", "unused");
            event.put("number", 9_007_199_254_740_993L);
            event.put("numbers", List.of(3L, 1L, 3L));
            event.put("nullable", null);
            seed(index, index.equals(FIRST) ? "A" : "B", event);
        }
        String pipeline = "EVAL kept=number*2 | WHERE kept>0 AND MV_COUNT(numbers)==3 AND nullable IS NULL AND @timestamp IS NOT NULL";
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a | %s | KEEP key,kept,numbers,nullable,@timestamp),
            (FROM sequence-retention-b | %s | KEEP key,kept,numbers,nullable,@timestamp)
            | EVAL other_kept=MEMBER(2,kept), other_numbers=MEMBER(2,numbers)
            | KEEP kept,numbers,nullable,@timestamp,other_kept,other_numbers | LIMIT 1
            """.formatted(pipeline, pipeline);
        for (String preference : List.of("NONE", "STORED")) {
            List<Object> left = ordinary(FIRST, pipeline + " | KEEP kept,numbers,nullable,@timestamp", preference).getFirst();
            List<Object> right = ordinary(SECOND, pipeline + " | KEEP kept,numbers", preference).getFirst();
            assertEquals(18_014_398_509_481_986L, left.getFirst());
            assertEquals(BASE.toString(), left.get(3));
            List<Object> expected = new ArrayList<>(left);
            expected.addAll(right);
            Observed result = observe(query, preference);
            assertEquals(List.of(expected), result.values());
            assertTrue(result.warnings().isEmpty());
            assertEquals(2L, result.status().stageDrivers());
        }
    }

    /** Visited unmatched rows can fill the warning budget before a selected row; source filters can exclude them. */
    public void testUnmatchedOutputWarningsShareTheFirstReadBudget() {
        createIndex(FIRST, false);
        createIndex(SECOND, false);
        var bulk = client().prepareBulk();
        for (int event = 0; event < 25; event++) {
            bulk.add(
                prepareIndex(FIRST).setId("unmatched-" + event)
                    .setSource(source(event, "unmatched-" + event, "A", "unmatched-bad-" + event))
            );
        }
        bulk.add(prepareIndex(SECOND).setId("B").setSource(source(100, "shared", "B", "unused")));
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a %s | EVAL converted=TO_LONG(payload) | KEEP key,converted),
            (FROM sequence-retention-b | KEEP key)
            | KEEP key,converted | LIMIT 1
            """;
        seed(FIRST, "selected", source(50, "shared", "A", "7"));
        Observed valid = observe(query.formatted(""), "NONE");
        assertEquals(List.of(List.of("shared", 7L)), valid.values());
        assertEquals(20, valid.warnings().stream().filter(warning -> warning.contains("Cannot parse number [unmatched-bad-")).count());
        assertEquals(1, valid.warnings().stream().filter(warning -> warning.contains("Only first 20 failures recorded.")).count());
        seed(FIRST, "selected", source(50, "shared", "A", "selected-bad"));
        Observed invalid = observe(query.formatted(""), "NONE");
        assertEquals(List.of(Arrays.asList("shared", null)), invalid.values());
        assertEquals(valid.warnings(), invalid.warnings());
        assertFalse(invalid.warnings().stream().anyMatch(warning -> warning.contains("selected-bad")));
        assertEquals(1, invalid.warnings().stream().filter(warning -> warning.contains("Only first 20 failures recorded.")).count());
        assertEquals(2L, invalid.status().stageDrivers());

        Observed filtered = observe(query.formatted("| WHERE key==\"shared\""), "NONE");
        assertEquals(invalid.values(), filtered.values());
        assertEquals(1, filtered.warnings().stream().filter(warning -> warning.contains("Cannot parse number [selected-bad]")).count());
        assertFalse(filtered.warnings().stream().anyMatch(warning -> warning.contains("unmatched-bad")));
        assertEquals(1, filtered.warnings().stream().filter(warning -> warning.contains("Only first 20 failures recorded.")).count());
        assertEquals(2L, filtered.status().stageDrivers());
    }

    public void testSelectionDependentWarningsKeepTheirCandidateAllowance() {
        createIndex(FIRST, false);
        createIndex(SECOND, false);
        var bulk = client().prepareBulk();
        for (int event = 0; event < 700; event++) {
            bulk.add(prepareIndex(FIRST).setId("A" + event).setSource(source(event, "shared", "A", "bad-" + event)));
        }
        bulk.add(prepareIndex(SECOND).setId("B").setSource(source(1000, "shared", "B", "unused")));
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a | EVAL converted=TO_LONG(payload)
              | WHERE COALESCE(converted,-1)==-1 | KEEP key,converted),
            (FROM sequence-retention-b | KEEP key)
            | KEEP key,converted | LIMIT 1
            """;
        Observed result = observe(query, "NONE");
        assertEquals(List.of(Arrays.asList("shared", null)), result.values());
        assertEquals(20, result.warnings().stream().filter(warning -> warning.contains("Cannot parse number [bad-")).count());
        assertEquals(1, result.warnings().stream().filter(warning -> warning.contains("Only first 20 failures recorded.")).count());
        assertEquals(result.warnings(), observe(query, "NONE").warnings());
    }

    /** Demanded outputs can fail before any match, while discarded outputs require neither loading nor evaluation. */
    public void testOutputOnlyRuntimeFailureOccursAtFirstReadAndPrunedValuesStayAbsent() {
        createIndex(FIRST, true);
        createIndex(SECOND, false);
        seed(FIRST, "A", source(0, "shared", "A", "bad"));
        seed(SECOND, "B", source(1, "shared", "B", "unused"));
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a | KEEP key,failure),
            (FROM sequence-retention-b | WHERE phase=="%s" | KEEP key)
            | KEEP key,failure | LIMIT 1
            """;
        RuntimeException plain = expectThrows(RuntimeException.class, () -> ordinary(FIRST, "KEEP key,failure", "NONE"));
        assertThat(ExceptionsHelper.stackTrace(plain), containsString("sequence mapped loading failure"));
        for (String phase : List.of("missing", "B")) {
            RuntimeException failure = expectThrows(RuntimeException.class, () -> observe(query.formatted(phase), "NONE"));
            assertEquals(loadingErrors(plain), loadingErrors(failure));
        }
        Observed pruned = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a | EVAL discarded=TO_LONG(payload) | DROP discarded | KEEP key),
            (FROM sequence-retention-b | KEEP key)
            | KEEP key | LIMIT 1
            """, "NONE");
        assertEquals(List.of(List.of("shared")), pruned.values());
        assertTrue(pruned.warnings().isEmpty());
        assertEquals(2L, pruned.status().stageDrivers());
    }

    /** LIMIT does not protect a demanded value already loaded in a candidate page before matching. */
    public void testLimitOneCannotHideRuntimeFailureOnTheFirstReadPage() {
        String good = FIRST + "-good", bad = FIRST + "-bad";
        createIndex(good, false);
        createIndex(bad, true);
        createIndex(SECOND, false);
        Map<String, Object> first = source(0, "good", "A", "unused");
        first.put("failure", 7L);
        seed(good, "A-good", first);
        seed(bad, "A-bad", source(2, "bad", "A", "unused"));
        seed(SECOND, "B-good", source(1, "good", "B", "unused"));
        seed(SECOND, "B-bad", source(3, "bad", "B", "unused"));
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a-* | KEEP key,failure), (FROM sequence-retention-b | KEEP key)
            | KEEP key,failure | LIMIT %s
            """;
        RuntimeException plain = expectThrows(RuntimeException.class, () -> ordinary(bad, "KEEP key,failure", "NONE"));
        assertThat(ExceptionsHelper.stackTrace(plain), containsString("sequence mapped loading failure"));
        for (int limit : List.of(1, 2)) {
            RuntimeException failure = expectThrows(RuntimeException.class, () -> observe(query.formatted(limit), "NONE"));
            assertEquals(loadingErrors(plain), loadingErrors(failure));
        }
        Observed limited = observe(query.formatted(1).replace("sequence-retention-a-*", good), "NONE");
        assertEquals(List.of(List.of("good", 7L)), limited.values());
        assertTrue(limited.warnings().isEmpty());
        assertEquals(ordinary(good, "KEEP key,failure", "NONE"), limited.values());
    }

    public void testProcessedMetadataNamesAndExplicitSourceRemainOrdinaryValues() {
        createIndex(FIRST, false);
        createIndex(SECOND, false);
        Map<String, Object> first = source(0, "shared", "A", "first");
        Map<String, Object> second = source(1, "shared", "B", "second");
        seed(FIRST, "A", first);
        seed(SECOND, "B", second);
        Observed changed = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a METADATA _id,_index,_source
              | EVAL _id=TO_UPPER(payload),_index="processed-index",_source="processed-source"
              | KEEP key,_id,_index,_source),
            (FROM sequence-retention-b | KEEP key)
            | KEEP _id,_index,_source | LIMIT 1
            """, "NONE");
        assertEquals(List.of(List.of("FIRST", "processed-index", "processed-source")), changed.values());
        Observed original = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-retention-a METADATA _source | RENAME _source AS body | KEEP key,body),
            (FROM sequence-retention-b | KEEP key)
            | KEEP body | LIMIT 1
            """, "NONE");
        assertEquals(List.of(List.of(first)), original.values());
        assertEquals(2L, original.status().stageDrivers());
    }

    /** Predicate-dependent and output-only branches retain their own processed values without a selected-row replay. */
    public void testMixedUnionBranchesRetainFirstReadValuesInEitherOrder() {
        String third = "sequence-retention-c";
        createIndex(FIRST, false);
        createIndex(SECOND, false);
        createIndex(third, false);
        seed(FIRST, "A", source(0, "one", "A", "ALPHA"));
        seed(third, "C", source(1, "two", "A", "beta"));
        seed(SECOND, "B-one", source(2, "one", "B", "unused"));
        seed(SECOND, "B-two", source(3, "two", "B", "unused"));
        String filtered = "(FROM sequence-retention-a | EVAL value=TO_LOWER(payload) | WHERE value!=\"skip\" | KEEP key,value)";
        String outputOnly = "(FROM sequence-retention-c | EVAL value=TO_UPPER(payload) | KEEP key,value)";
        for (boolean reverse : List.of(false, true)) {
            String query = """
                SEQUENCE BY key WITHIN 30s PRIMARY 1
                (FROM %s,%s), (FROM sequence-retention-b | KEEP key)
                | KEEP key,value | LIMIT 10
                """.formatted(reverse ? outputOnly : filtered, reverse ? filtered : outputOnly);
            Observed result = observe(query, "NONE");
            assertEquals(List.of(List.of("one", "alpha"), List.of("two", "BETA")), result.values());
            assertTrue(result.warnings().isEmpty());
            assertEquals(3L, result.status().stageDrivers());
        }
    }

    private Observed observe(String query, String preference) {
        var coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var future = new PlainActionFuture<ResponseCopy>();
        EsqlQueryRequest request = syncEsqlQueryRequest(query).allowPartialResults(false).profile(true).pragmas(pragmas(preference));
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                ResponseCopy copy;
                // The action releases its callback reference. Own the independent reference closed after copying below.
                response.incRef();
                try (response) {
                    List<List<Object>> values = getValuesList(response);
                    List<SequenceSourceStatus> statuses = response.profile() == null
                        ? List.of()
                        : response.profile()
                            .drivers()
                            .stream()
                            .flatMap(driver -> driver.operators().stream())
                            .map(OperatorStatus::status)
                            .filter(SequenceSourceStatus.class::isInstance)
                            .map(SequenceSourceStatus.class::cast)
                            .toList();
                    var context = internalCluster().getInstance(TransportService.class, coordinator.getName())
                        .getThreadPool()
                        .getThreadContext();
                    List<String> warnings = context.getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                        .filter(warning -> warning.startsWith("Line "))
                        .toList();
                    copy = new ResponseCopy(values, warnings, statuses, response.isPartial());
                }
                future.onResponse(copy);
            } catch (AssertionError failure) {
                // ActionListener.wrap catches Exception, so an unexpected copy assertion also needs a terminal future.
                future.onFailure(new IllegalStateException("SEQUENCE retention response copying failed", failure));
            }
        }, future::onFailure));
        ResponseCopy copy = future.actionGet(DEFAULT_REQUEST_TIMEOUT);
        assertFalse(copy.partial());
        assertEquals("one native source profile is required", 1, copy.statuses().size());
        return new Observed(copy.values(), copy.warnings(), copy.statuses().getFirst());
    }

    private List<List<Object>> ordinary(String index, String pipeline, String preference) {
        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + index + " | " + pipeline + " | LIMIT 10").allowPartialResults(false)
                    .pragmas(pragmas(preference))
            )
        ) {
            return getValuesList(response);
        }
    }

    /** Search shard roots and ordinary data-node failures use different wrappers around the same loading error. */
    private static List<String> loadingErrors(Exception failure) {
        return Arrays.stream(ElasticsearchException.guessRootCauses(failure))
            .map(root -> ExceptionsHelper.unwrapCause(root).getMessage())
            .distinct()
            .sorted()
            .toList();
    }

    private static QueryPragmas pragmas(String preference) {
        return new QueryPragmas(Settings.builder().put("page_size", 17).put("field_extract_preference", preference).build());
    }

    private static Map<String, Object> source(long millis, String key, String phase, Object payload) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("@timestamp", BASE.plusMillis(millis).toString());
        values.put("key", key);
        values.put("phase", phase);
        values.put("payload", payload);
        return values;
    }

    private void seed(String index, String id, Map<String, Object> source) {
        prepareIndex(index).setId(id).setSource(source).get();
        refresh(index);
    }

    private void createIndex(String index, boolean runtimeFailure) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("@timestamp", Map.of("type", "date_nanos"));
        for (String keyword : List.of("key", "phase", "payload", "nullable")) {
            properties.put(keyword, Map.of("type", "keyword", "store", true));
        }
        for (String number : List.of("number", "numbers", "failure")) {
            properties.put(number, Map.of("type", "long", "store", true));
        }
        properties.put("body", Map.of("type", "object", "enabled", false));
        Map<String, Object> mapping = new LinkedHashMap<>();
        mapping.put("properties", properties);
        if (runtimeFailure) {
            mapping.put(
                "runtime",
                Map.of("failure", Map.of("type", "long", "script", Map.of("lang", "sequence_failure", "source", "failure")))
            );
        }
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping(mapping)
        );
    }

    @After
    public void assertRetentionResourcesReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

    /** All response-owned data is copied before release; warning headers come from the selected coordinator thread. */
    private record Observed(List<List<Object>> values, List<String> warnings, SequenceSourceStatus status) {}

    /** Callback copies only immutable response data; all assertions and retained-row decoding run on the test thread. */
    private record ResponseCopy(List<List<Object>> values, List<String> warnings, List<SequenceSourceStatus> statuses, boolean partial) {}
}
