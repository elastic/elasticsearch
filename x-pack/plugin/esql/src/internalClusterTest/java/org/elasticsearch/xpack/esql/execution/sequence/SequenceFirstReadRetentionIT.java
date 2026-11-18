/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** First-read capture preserves computed, expanded and source values without re-reading matched events. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceFirstReadRetentionIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");
    private static final String FIRST = "sequence-first-read-a";
    private static final String SECOND = "sequence-first-read-b";

    /** Predicate values can close on commit while independent output handles retain computed values of either stage. */
    public void testE5ComputedNonPrimaryMemberRetainedAtFirstRead() {
        createIndices();
        seed(FIRST, "a", event(0, "ALPHA"));
        seed(SECOND, "b", event(1, "ALPHA"));
        for (int primary : List.of(1, 2)) {
            Observed result = observe("""
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM sequence-first-read-a | EVAL value=TO_LOWER(payload) | KEEP key,value),
                (FROM sequence-first-read-b | EVAL value=CONCAT(TO_LOWER(payload),"/b")
                  | WHERE TO_LOWER(payload)==MEMBER(1,value) | KEEP key,value)
                | EVAL other=MEMBER(%s,value) | KEEP value,other | LIMIT 10
                """.formatted(primary, primary == 1 ? 2 : 1));
            assertEquals(List.of(primary == 1 ? List.of("alpha", "alpha/b") : List.of("alpha/b", "alpha")), result.values());
            assertFirstReadOnly(result);
            assertTrue(result.status().latestValidCounters().conditionEvaluations() > 0);
        }
    }

    /** Expanded siblings keep the selected processed values without another document read. */
    public void testE5ExpandedRowsRetainTheSelectedValues() {
        createIndices();
        seed(FIRST, "a", event(0, List.of("beta", "alpha")));
        seed(SECOND, "b-alpha", event(10, "alpha"));
        seed(SECOND, "b-beta", event(11, "beta"));
        Observed result = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-first-read-a | MV_EXPAND payload | EVAL value=CONCAT(payload,"/a") | KEEP key,value),
            (FROM sequence-first-read-b | EVAL expected=CONCAT(payload,"/a")
              | WHERE expected==MEMBER(1,value) | KEEP key,payload)
            | EVAL final=MEMBER(2,payload) | KEEP value,final | LIMIT 10
            """);
        assertEquals(List.of(List.of("alpha/a", "alpha"), List.of("beta/a", "beta")), result.values());
        assertEquals(2, result.status().completedMatches());
        assertFirstReadOnly(result);
    }

    /** Either PRIMARY choice keeps the original nested source from the first authorized read. */
    public void testE5ExplicitSourceRetainedAtFirstRead() {
        createIndices();
        Map<String, Object> first = event(0, "left");
        first.put("body", Map.of("array", List.of(1, 2), "nested", Map.of("value", "first")));
        Map<String, Object> second = event(1, "right");
        second.put("body", Map.of("array", List.of(3, 4), "nested", Map.of("value", "second")));
        seed(FIRST, "a", first);
        seed(SECOND, "b", second);
        for (int primary : List.of(1, 2)) {
            Observed result = observe("""
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM sequence-first-read-a METADATA _source | RENAME _source AS original | KEEP key,original),
                (FROM sequence-first-read-b METADATA _source | RENAME _source AS original | KEEP key,original)
                | KEEP original | LIMIT 10
                """.formatted(primary));
            assertEquals(List.of(List.of(primary == 1 ? first : second)), result.values());
            assertFirstReadOnly(result);
        }
    }

    /** Pruned output computations do not introduce extra field loading or retained values. */
    public void testE5OnlyDemandedOutputColumnsAreRetained() {
        createIndices();
        seed(FIRST, "a", event(0, "not-a-number"));
        seed(SECOND, "b", event(1, "unused"));
        Observed result = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-first-read-a | EVAL discarded=TO_LONG(payload),kept=CONCAT(key,"/needed")
              | DROP discarded | KEEP key,kept),
            (FROM sequence-first-read-b | KEEP key)
            | KEEP kept | LIMIT 10
            """);
        assertEquals(List.of(List.of("shared/needed")), result.values());
        assertFirstReadOnly(result);
    }

    /** The real cluster setting rejects the whole query in either profile mode, even when partial results were requested. */
    public void testDynamicMemoryBudgetFailsWithoutPartialResultsForEitherProfileMode() {
        createIndices();
        seed(FIRST, "a", event(0, "left"));
        seed(SECOND, "b", event(1, "right"));
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-first-read-a | KEEP key,payload), (FROM sequence-first-read-b | KEEP key)
            | KEEP key,payload | LIMIT 10
            """;
        String setting = org.elasticsearch.xpack.esql.planner.PlannerSettings.SEQUENCE_MAX_MEMORY.getKey();
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(setting, "1b"))
                .get()
        );
        try {
            for (boolean profile : List.of(false, true)) {
                Exception failure = expectThrows(Exception.class, () -> {
                    try (var response = run(syncEsqlQueryRequest(query).profile(profile).allowPartialResults(true))) {
                        getValuesList(response);
                    }
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString("SEQUENCE memory limit [" + setting + "]"));
                assertThat(ExceptionsHelper.stackTrace(failure), containsString("limit [1] bytes"));
            }
        } finally {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(setting))
                    .get()
            );
        }
        try (var response = run(syncEsqlQueryRequest(query).allowPartialResults(false))) {
            assertEquals(List.of(List.of("shared", "left")), getValuesList(response));
        }
    }

    private static void assertFirstReadOnly(Observed result) {
        assertEquals(2, result.status().stageDrivers());
        assertTrue(result.warnings().isEmpty());
    }

    private Observed observe(String query) {
        var coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var future = new PlainActionFuture<Observed>();
        var request = syncEsqlQueryRequest(query).allowPartialResults(false)
            .profile(true)
            .pragmas(new QueryPragmas(Settings.builder().put("page_size", 17).build()));
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<SequenceSourceStatus> statuses = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            var context = internalCluster().getInstance(TransportService.class, coordinator.getName()).getThreadPool().getThreadContext();
            List<String> warnings = context.getResponseHeaders()
                .getOrDefault("Warning", List.of())
                .stream()
                .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                .filter(warning -> warning.startsWith("Line ") || warning.startsWith("SEQUENCE "))
                .toList();
            if (statuses.size() != 1) {
                future.onFailure(new IllegalStateException("E5 gate expected one native source profile"));
                return;
            }
            future.onResponse(new Observed(getValuesList(response), statuses.getFirst(), warnings));
        }, future::onFailure));
        return future.actionGet(DEFAULT_REQUEST_TIMEOUT);
    }

    private void createIndices() {
        for (String index : List.of(FIRST, SECOND)) {
            assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(2, 0)).setMapping("""
                {"properties":{"@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},
                  "payload":{"type":"keyword"},"body":{"type":"object","enabled":false}}}
                """));
        }
    }

    private void seed(String index, String id, Map<String, Object> source) {
        prepareIndex(index).setId(id).setSource(source).get();
        refresh(index);
    }

    private static Map<String, Object> event(long millis, Object payload) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("@timestamp", BASE.plusMillis(millis).toString());
        event.put("key", "shared");
        event.put("payload", payload);
        return event;
    }

    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        assertBusy(
            () -> assertTrue(
                "query, stage and search tasks must drain after admission failure",
                clusterAdmin().prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME, TransportSearchAction.TYPE.name() + "*")
                    .get()
                    .getTasks()
                    .isEmpty()
            )
        );
        ensureBlocksReleased();
    }

    private record Observed(List<List<Object>> values, SequenceSourceStatus status, List<String> warnings) {}
}
