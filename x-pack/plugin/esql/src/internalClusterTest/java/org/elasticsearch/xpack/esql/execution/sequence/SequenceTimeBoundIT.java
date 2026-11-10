/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;

/** Verifies the initial lower time bound preserves selection and excludes only earlier later-stage input. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceTimeBoundIT extends AbstractSequenceIntegTestCase {
    private static final String A = "sequence-time-bound-a";
    private static final String B = "sequence-time-bound-b";
    private static final String C = "sequence-time-bound-c";
    private static final String D = "sequence-time-bound-d";
    private static final Instant BASE = Instant.parse("2026-11-06T12:00:00Z");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(TrackingEsqlPlugin.class);
        return plugins;
    }

    /** Uses the real allocator and adds the existing test block-ownership checks. */
    public static class TrackingEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    /** Earlier later-stage pages must not be read when none of their events can follow an eligible start. */
    public void testSkipsEarlierLaterStagePages() throws Exception {
        create("date", 2, A, B);
        var bulk = client().prepareBulk();
        List<List<Object>> expected = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            String key = "key-" + (1000 + i);
            add(bulk, A, "A" + i, BASE.plusMillis(10000 + i * 2).toString(), key, "yes", "7");
            add(bulk, B, "B" + i, BASE.plusMillis(11000 + i * 2).toString(), key, "yes", "7");
            expected.add(List.of(key, "A" + i, "B" + i));
        }
        for (int i = 0; i < 1536; i++) {
            add(bulk, B, "early-" + i, BASE.minusSeconds(20).plusMillis(i).toString(), "key-" + (1000 + i % 16), "yes", "7");
        }
        finishSeed(bulk, A, B);
        String query = ordinary();
        Outcome result = observe(query);
        assertRows(expected, result);
        assertEquals(32L, result.status.candidateRowsFetched());
        assertEquals(2L, result.status.candidateSearchRequests());
        assertEquals(32L, result.status.payloadRowsFetched());
    }

    /** A later-inspected branch can contain the earliest eligible start after several filtered pages. */
    public void testFilteredFirstStageAndAllBranches() throws Exception {
        create("date", 2, A, B, C, D);
        var bulk = client().prepareBulk();
        add(bulk, A, "ignored-A", BASE.minusSeconds(30).toString(), "ignored", "NO", "7");
        add(bulk, A, "late-A", BASE.plusSeconds(12).toString(), "late", "YES", "7");
        for (int i = 0; i < 300; i++) {
            add(bulk, C, "filtered-" + i, BASE.plusMillis(i).toString(), "filtered", "NO", "7");
        }
        add(bulk, C, "early-C", BASE.plusSeconds(10).toString(), "early", "YES", "7");
        add(bulk, B, "early-B", BASE.plusMillis(10500).toString(), "early", "yes", "7");
        add(bulk, B, "late-B", BASE.plusMillis(12500).toString(), "late", "yes", "7");
        add(bulk, B, "too-early-B", BASE.plusSeconds(9).toString(), "early", "yes", "7");
        finishSeed(bulk, A, B, C, D);
        String first = "from (" + branch(A) + "), (" + branch(C) + "), (" + branch(D) + ")";
        verify(sequence(first, stage(B)), List.of(List.of("early", "early-C", "early-B"), List.of("late", "late-A", "late-B")));
    }

    /** Rounding the bound to milliseconds would read or discard the wrong nanosecond events. */
    public void testNanosecondPrecision() throws Exception {
        create("date_nanos", 1, A, B);
        Instant first = BASE.plusNanos(123456789);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", first.toString(), "key", "yes", "7");
        add(bulk, B, "before", first.minusNanos(1).toString(), "key", "yes", "7");
        add(bulk, B, "tied-other-key", first.toString(), "other", "yes", "7");
        add(bulk, B, "after", first.plusNanos(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        Outcome bounded = verify(ordinary(), List.of(List.of("key", "A", "after")));
        assertEquals(3L, bounded.status.candidateRowsFetched());
    }

    /** The hidden source-time bound must not inherit a mapping that accepts only numeric epoch timestamps. */
    public void testEpochMillisTimestampFormatWithHiddenSourceTime() throws Exception {
        create("date", "epoch_millis", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", 0L, "key", "yes", "7");
        add(bulk, B, "before", -1L, "key", "yes", "7");
        add(bulk, B, "B", 1000L, "key", "yes", "7");
        finishSeed(bulk, A, B);

        // Both stages project away @timestamp; no request filter can supply or mask the generated lower bound.
        Outcome result = observe(syncEsqlQueryRequest(ordinary()).profile(true));
        assertRows(List.of(List.of("key", "A", "B")), result);
        assertEquals(2L, result.status.candidateRowsFetched());
        assertEquals(2L, result.status.candidateSearchRequests());
        assertEquals(2L, result.status.payloadRowsFetched());
    }

    /** An ordinary date before the epoch can precede a valid nanosecond event within the same sequence. */
    public void testDateStartBeforeEpochMatchesNanosecondStage() throws Exception {
        create("date", 1, A);
        create("date_nanos", 1, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", "1969-12-31T23:59:59Z", "key", "yes", "7");
        add(bulk, B, "B", "1970-01-01T00:00:05Z", "key", "yes", "7");
        finishSeed(bulk, A, B);

        // No request filter may exclude the boundary dates before the generated stage bound is evaluated.
        Outcome result = observe(syncEsqlQueryRequest(ordinary()).profile(true));
        assertRows(List.of(List.of("key", "A", "B")), result);
    }

    /** A start beyond the nanosecond domain makes a populated nanosecond stage ineligible without failing the query. */
    public void testDateStartAfterNanosecondRangeReturnsNoMatches() throws Exception {
        create("date", 1, A);
        create("date_nanos", 1, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", "2262-04-11T23:47:16.855Z", "key", "yes", "7");
        // An empty shard could bypass range parsing and hide the generated bound's invalid nanos conversion.
        add(bulk, B, "B", "2262-04-11T23:47:16.854775807Z", "key", "yes", "7");
        finishSeed(bulk, A, B);

        Outcome result = observe(syncEsqlQueryRequest(ordinary()).profile(true));
        assertRows(List.of(), result);
    }

    /** Pruning must retain predecessor replacement, expanded memberships and the inclusive total-span boundary. */
    public void testReplacementExpansionAndMaxspan() throws Exception {
        create("date", 2, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "old-A", BASE.plusMillis(9500).toString(), List.of("inside", "copy"), "yes", "7");
        add(bulk, A, "new-A", BASE.plusSeconds(10).toString(), List.of("inside", "copy"), "yes", "7");
        add(bulk, A, "outside-A", BASE.plusSeconds(10).toString(), "outside", "yes", "7");
        add(bulk, B, "too-early", BASE.toString(), "inside", "yes", "7");
        add(bulk, B, "boundary-B", BASE.plusSeconds(70).toString(), List.of("inside", "copy"), "yes", "7");
        add(bulk, B, "outside-B", BASE.plusMillis(70001).toString(), "outside", "yes", "7");
        finishSeed(bulk, A, B);
        verify(
            sequence(stage(A) + " | mv_expand key", stage(B) + " | mv_expand key"),
            List.of(List.of("copy", "new-A", "boundary-B"), List.of("inside", "new-A", "boundary-B"))
        );
    }

    /** An empty first stage retains the existing scan behavior instead of introducing a second optimization. */
    public void testNoEligibleFirstStage() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "ignored-A", BASE.toString(), "key", "NO", "7");
        add(bulk, B, "B", BASE.plusSeconds(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        Outcome last = verify(sequence(branch(A), stage(B)), List.of());
        assertEquals(2L, last.status.candidateRowsFetched());
    }

    /** An inclusive time bound leaves tied-event ordering to the existing PIT ordinal comparison. */
    public void testEqualTimestampCandidatesRemainReadable() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.toString(), "key", "yes", "7");
        add(bulk, B, "before", BASE.minusMillis(1).toString(), "key", "yes", "7");
        add(bulk, B, "tied", BASE.toString(), "key", "yes", "7");
        add(bulk, B, "after", BASE.plusMillis(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        Outcome result = observe(ordinary());
        assertOk(result);
        assertEquals(1, result.rows.size());
        assertTrue(List.of("tied", "after").contains(result.rows.getFirst().get(2)));
        assertEquals("same-timestamp B must still be read", 3L, result.status.candidateRowsFetched());
    }

    /** An event excluded before candidate loading cannot produce a sequence timestamp diagnostic. */
    public void testExcludedTimestampIsNotValidated() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.plusSeconds(10).toString(), "key", "yes", "7");
        add(bulk, B, "invalid-earlier-timestamp", List.of(BASE.toString(), BASE.plusSeconds(1).toString()), "key", "yes", "7");
        add(bulk, B, "B", BASE.plusSeconds(11).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        assertRows(List.of(List.of("key", "A", "B")), observe(ordinary()));
    }

    /** An expression on an excluded event must not be evaluated solely to reproduce its warning. */
    public void testExcludedExpressionIsNotEvaluated() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.plusSeconds(10).toString(), "key", "yes", "7");
        add(bulk, B, "bad-number-before-A", BASE.toString(), "key", "yes", "bad-number");
        add(bulk, B, "B", BASE.plusSeconds(11).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        String query = sequence(stage(A), "from " + B + " metadata _id | where COALESCE(TO_LONG(payload), -1)>=-1 | keep key, _id");
        assertRows(List.of(List.of("key", "A", "B")), observe(query));
    }

    /** The time bound must preserve the same middle member and enforce span across all three members. */
    public void testThreeStagesReplacementAndTotalMaxspan() throws Exception {
        create("date", 2, A, B, C);
        var bulk = client().prepareBulk();
        for (String key : List.of("good", "replace", "missing-middle", "reverse-middle", "boundary", "expired")) {
            add(bulk, A, key + "-A", BASE.plusSeconds(10).toString(), key, "yes", "7");
            if (key.equals("replace")) {
                add(bulk, A, "replace-new-A", BASE.plusMillis(10500).toString(), key, "yes", "7");
            }
            if (key.equals("missing-middle") == false) {
                long middle = key.equals("reverse-middle") ? 9000 : key.equals("boundary") || key.equals("expired") ? 20000 : 11000;
                add(bulk, B, key + "-B", BASE.plusMillis(middle).toString(), key, "yes", "7");
            }
            long last = key.equals("boundary") ? 70000 : key.equals("expired") ? 70001 : 12000;
            add(bulk, C, key + "-C", BASE.plusMillis(last).toString(), key, "yes", "7");
        }
        for (int i = 0; i < 256; i++) {
            add(bulk, B, "early-B" + i, BASE.plusMillis(i).toString(), "good", "yes", "7");
            add(bulk, C, "early-C" + i, BASE.plusMillis(i).toString(), "good", "yes", "7");
        }
        finishSeed(bulk, A, B, C);
        verify(
            chain(A, B, C),
            List.of(
                List.of("good", "good-A", "good-B", "good-C"),
                List.of("replace", "replace-new-A", "replace-B", "replace-C"),
                List.of("boundary", "boundary-A", "boundary-B", "boundary-C")
            )
        );
    }

    /** A fourth stage must not join partial or reversed chains, or change the selected competing predecessor. */
    public void testFourStagesCompetingMiddleAndMissingStage() throws Exception {
        create("date", 2, A, B, C, D);
        var bulk = client().prepareBulk();
        for (String key : List.of("good", "replace", "missing-third", "reverse-third")) {
            add(bulk, A, key + "-A", BASE.plusSeconds(10).toString(), key, "yes", "7");
            add(bulk, B, key + "-B", BASE.plusSeconds(key.equals("reverse-third") ? 12 : 11).toString(), key, "yes", "7");
            if (key.equals("missing-third") == false) {
                add(bulk, C, key + "-C", BASE.plusSeconds(key.equals("reverse-third") ? 11 : 12).toString(), key, "yes", "7");
            }
            if (key.equals("replace")) {
                add(bulk, C, "replace-new-C", BASE.plusMillis(12500).toString(), key, "yes", "7");
            }
            add(bulk, D, key + "-D", BASE.plusSeconds(13).toString(), key, "yes", "7");
        }
        for (int i = 0; i < 300; i++) {
            for (String index : List.of(B, C, D)) {
                add(bulk, index, "early-" + i, BASE.plusMillis(i).toString(), "good", "yes", "7");
            }
        }
        finishSeed(bulk, A, B, C, D);
        verify(
            chain(A, B, C, D),
            List.of(
                List.of("good", "good-A", "good-B", "good-C", "good-D"),
                List.of("replace", "replace-A", "replace-B", "replace-C", "replace-D")
            )
        );
    }

    private static String chain(String... indices) {
        StringBuilder query = new StringBuilder("sequence by key with {\"maxspan\":\"60s\"} ");
        for (int i = 0; i < indices.length; i++) {
            if (i > 0) query.append(", ");
            query.append("(").append(stage(indices[i])).append(")");
        }
        query.append(" | keep key");
        for (int i = 0; i < indices.length; i++)
            query.append(", stage").append(i + 1).append("._id");
        return query.append(" | sort key | limit 100").toString();
    }

    private Outcome verify(String query, List<List<Object>> expected) throws Exception {
        Outcome result = observe(query);
        assertRows(expected, result);
        return result;
    }

    private Outcome observe(String query) throws Exception {
        return observe(
            syncEsqlQueryRequest(query).profile(true)
                .filter(rangeQuery("@timestamp").gte(BASE.minusSeconds(60).toString()).lte(BASE.plusSeconds(120).toString()))
        );
    }

    private Outcome observe(EsqlQueryRequest request) throws Exception {
        String coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        var future = new PlainActionFuture<Outcome>();
        try {
            client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
                try {
                    assertFalse(response.isPartial());
                    List<List<Object>> rows = getValuesList(response);
                    assertTrue("output cap reached", rows.size() < 100);
                    var statuses = response.profile()
                        .drivers()
                        .stream()
                        .flatMap(driver -> driver.operators().stream())
                        .map(OperatorStatus::status)
                        .filter(SequenceSourceStatus.class::isInstance)
                        .map(SequenceSourceStatus.class::cast)
                        .toList();
                    assertEquals(1, statuses.size());
                    SequenceSourceStatus status = statuses.getFirst();
                    var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
                    var warnings = context.getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(w -> HeaderWarning.extractWarningValueFromWarningHeader(w, false))
                        .filter(w -> w.startsWith("Line "))
                        .toList();
                    future.onResponse(new Outcome(rows, status, warnings, ""));
                } catch (Exception e) {
                    future.onFailure(e);
                } catch (AssertionError e) {
                    future.onFailure(new IllegalStateException(e));
                }
            }, e -> future.onResponse(new Outcome(List.of(), null, List.of(), errorChain(e)))));
            return future.actionGet(TimeValue.timeValueMinutes(2));
        } finally {
            assertReleased();
        }
    }

    private static String errorChain(Throwable error) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; error != null && i < 12; i++, error = error.getCause()) {
            messages.add(error.getClass().getSimpleName() + ": " + error.getMessage());
        }
        return String.join(" / ", messages);
    }

    private static String stage(String index) {
        return "from " + index + " metadata _id | where disposition==\"yes\" | keep key, _id";
    }

    private static String branch(String index) {
        return "from " + index + " metadata _id | eval accepted=LENGTH(disposition)+TO_LONG(payload) | where accepted==10 | keep key, _id";
    }

    private static String sequence(String first, String second) {
        return "sequence by key with {\"maxspan\":\"60s\"} ("
            + first
            + "), ("
            + second
            + ") | keep key, stage1._id, stage2._id | sort key | limit 100";
    }

    private static String ordinary() {
        return sequence(stage(A), stage(B));
    }

    private void create(String timestampType, int shards, String... indices) {
        create(timestampType, null, shards, indices);
    }

    private void create(String timestampType, String timestampFormat, int shards, String... indices) {
        for (String index : indices) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(shards, 0))
                    .setMapping(
                        "@timestamp",
                        "type=" + timestampType + (timestampFormat == null ? "" : ",format=" + timestampFormat),
                        "key",
                        "type=keyword",
                        "disposition",
                        "type=keyword",
                        "payload",
                        "type=keyword"
                    )
            );
        }
        ensureGreen(indices);
    }

    private void add(BulkRequestBuilder bulk, String index, String id, Object timestamp, Object key, String disposition, String payload) {
        bulk.add(
            prepareIndex(index).setId(id)
                .setSource(Map.of("@timestamp", timestamp, "key", key, "disposition", disposition, "payload", payload))
        );
    }

    private void finishSeed(BulkRequestBuilder bulk, String... indices) {
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        refresh(indices);
    }

    private static void assertOk(Outcome outcome) {
        assertEquals("", outcome.error);
        assertEquals(List.of(), outcome.warnings);
    }

    private static void assertRows(List<List<Object>> expected, Outcome outcome) {
        assertOk(outcome);
        assertEquals(
            expected.stream().sorted(Comparator.comparing(Object::toString)).toList(),
            outcome.rows.stream().sorted(Comparator.comparing(Object::toString)).toList()
        );
    }

    private void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class))
                assertEquals(0, service.getActiveContexts());
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals(0L, service.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
            assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME)
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    /** Each test must release owned resources before its fixture is removed. */
    @After
    public void cleanup() throws Exception {
        assertReleased();
        for (String index : List.of(A, B, C, D)) {
            if (indexExists(index)) assertAcked(indicesAdmin().prepareDelete(index));
        }
    }

    private record Outcome(List<List<Object>> rows, SequenceSourceStatus status, List<String> warnings, String error) {}
}
