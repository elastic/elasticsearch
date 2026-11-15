/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
    private static final String E = "sequence-time-bound-e";
    private static final String F = "sequence-time-bound-f";
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

        /** Copy only immutable generated-candidate request data, preserving the ordinary listener and chain. */
        @Override
        public Collection<org.elasticsearch.action.support.ActionFilter> getActionFilters() {
            var filters = new ArrayList<>(super.getActionFilters());
            filters.add(new org.elasticsearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public <
                    Request extends org.elasticsearch.action.ActionRequest,
                    Response extends org.elasticsearch.action.ActionResponse> void apply(
                        org.elasticsearch.tasks.Task task,
                        String action,
                        Request request,
                        ActionListener<Response> listener,
                        org.elasticsearch.action.support.ActionFilterChain<Request, Response> chain
                    ) {
                    CandidateRequests capture = CandidateRequests.ACTIVE.get();
                    if (capture != null
                        && request instanceof SearchRequest search
                        && search.getPointInTimeAuthorizationScope() != null
                        && search.source() != null
                        && search.source().sorts() != null
                        && search.source().sorts().size() == 2
                        && search.source().sorts().getFirst() instanceof FieldSortBuilder temporal) {
                        Object[] after = search.source().searchAfter();
                        capture.requests.add(
                            new CandidateRequest(
                                List.of(search.getPointInTimeAuthorizationScope().indices()),
                                org.elasticsearch.common.Strings.toString(search.source().query()),
                                after == null ? List.of() : java.util.Collections.unmodifiableList(Arrays.asList(after.clone())),
                                temporal.getNumericType()
                            )
                        );
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
            return filters;
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
        // Selection already owns the key; untouched original IDs need no late fields or replay.
        assertEquals(0L, result.status.payloadRowsFetched());
        assertEquals(0L, result.status.payloadSearchRequests());
        assertEquals(2L, result.status.stageDrivers());
        assertEquals(16L, result.status.completedMatches());
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
        // The hidden timestamp stays native at selection; retained key/ID output does not demand it again.
        assertEquals(0L, result.status.payloadRowsFetched());
        assertEquals(0L, result.status.payloadSearchRequests());
        assertEquals(2L, result.status.stageDrivers());
        assertEquals(1L, result.status.completedMatches());
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

    /** Once the computed first-stage filter rejects its only row, no later candidate can participate. */
    public void testNoEligibleFirstStage() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "ignored-A", BASE.toString(), "key", "NO", "7");
        add(bulk, B, "B", BASE.plusSeconds(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        Outcome last = verify(sequence(branch(A), stage(B)), List.of());
        assertEquals(1L, last.status.candidateRowsFetched());
        assertEquals(1L, last.status.candidateSearchRequests());
    }

    /** An empty but mapped first source needs one search and no later candidate work. */
    public void testEmptyFirstStageSkipsLaterCandidates() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, B, "B", BASE.plusSeconds(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        Outcome result = verify(ordinary(), List.of());
        assertEquals(0L, result.status.candidateRowsFetched());
        assertEquals(1L, result.status.candidateSearchRequests());
    }

    /** A missing key cannot create a prefix, even when the event passes its stage filter. */
    public void testNullKeyFirstStageSkipsLaterCandidates() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        bulk.add(
            prepareIndex(A).setId("missing-key").setSource(Map.of("@timestamp", BASE.toString(), "disposition", "yes", "payload", "7"))
        );
        add(bulk, B, "B", BASE.plusSeconds(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        Outcome result = verify(ordinary(), List.of());
        assertEquals(1L, result.status.candidateRowsFetched());
        assertEquals(1L, result.status.candidateSearchRequests());
    }

    /** The finalized start horizon excludes a filtered late tail before its first page is fetched. */
    public void testFilteredTailWithoutLaterHeadIsBounded() throws Exception {
        verifyFilteredTail(false);
    }

    /** An in-window unrelated head remains readable while the finalized horizon excludes the later filtered tail. */
    public void testFilteredTailAfterNonmatchingHeadIsBounded() throws Exception {
        verifyFilteredTail(true);
    }

    private void verifyFilteredTail(boolean includeHead) throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.toString(), "key", "yes", "7");
        if (includeHead) {
            add(bulk, B, "unrelated-head", BASE.plusSeconds(1).toString(), "other", "yes", "7");
        }
        for (int i = 0; i < 1536; i++) {
            add(bulk, B, "filtered-" + i, BASE.plusSeconds(61).plusMillis(i).toString(), "key", "NO", "7");
        }
        finishSeed(bulk, A, B);
        Outcome result = verify(sequence(stage(A), branch(B)), List.of());
        assertEquals("read only the start and bounded later source", 2L, result.status.candidateSearchRequests());
        assertEquals(includeHead ? 2L : 1L, result.status.candidateRowsFetched());
    }

    /** A far-future raw page end cannot expire an earlier valid completion in the same page. */
    public void testMixedPageTailKeepsEarlyCompletion() throws Exception {
        create("date_nanos", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.toString(), "key", "yes", "7");
        add(bulk, B, "B", BASE.plusSeconds(60).toString(), "key", "yes", "7");
        for (int i = 0; i < 512; i++) {
            add(bulk, B, "filtered-" + i, BASE.plusSeconds(60).plusNanos(i + 1).toString(), "key", "NO", "7");
        }
        finishSeed(bulk, A, B);
        Outcome result = verify(sequence(stage(A), branch(B)), List.of(List.of("key", "A", "B")));
        assertEquals(2L, result.status.candidateSearchRequests());
    }

    /** One filtered branch looking ahead must not discard the sibling branch's valid candidate. */
    public void testFilteredTailKeepsSiblingCompletion() throws Exception {
        create("date", 1, A, B, C);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.toString(), "key", "yes", "7");
        add(bulk, C, "C", BASE.plusSeconds(5).toString(), "key", "yes", "7");
        for (int i = 0; i < 768; i++) {
            add(bulk, B, "filtered-" + i, BASE.plusSeconds(61).plusMillis(i).toString(), "key", "NO", "7");
        }
        finishSeed(bulk, A, B, C);
        String later = "from (" + branch(B) + "), (" + stage(C) + ")";
        Outcome result = verify(sequence(stage(A), later), List.of(List.of("key", "A", "C")));
        assertEquals(3L, result.status.candidateSearchRequests());
    }

    /** A filtered reader cannot use a latest-start bound while another start page is still unread. */
    public void testFilteredTailCannotSkipUnreadLaterStart() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 256; i++) {
            add(bulk, A, "early-A" + i, BASE.plusMillis(i).toString(), "unmatched-" + i, "yes", "7");
        }
        add(bulk, A, "late-A", BASE.plusSeconds(70).toString(), "late", "yes", "7");
        add(bulk, B, "early-unrelated-B", BASE.plusMillis(1).toString(), "other", "yes", "7");
        for (int i = 0; i < 255; i++) {
            add(bulk, B, "filtered-" + i, BASE.plusSeconds(61).plusMillis(i).toString(), "other", "NO", "7");
        }
        add(bulk, B, "late-B", BASE.plusSeconds(80).toString(), "late", "yes", "7");
        finishSeed(bulk, A, B);
        verify(sequence(stage(A), branch(B)), List.of(List.of("late", "late-A", "late-B")));
    }

    /** Whole-source stopping still delivers a final batch smaller than its capacity. */
    public void testCompletedTailKeepsPartialOutputBatch() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        List<List<Object>> expected = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String key = "key-" + i;
            add(bulk, A, "A" + i, BASE.plusMillis(i).toString(), key, "yes", "7");
            add(bulk, B, "B" + i, BASE.plusSeconds(1).plusMillis(i).toString(), key, "yes", "7");
            expected.add(List.of(key, "A" + i, "B" + i));
        }
        for (int i = 0; i < 768; i++) {
            add(bulk, B, "tail-" + i, BASE.plusSeconds(61).plusMillis(i).toString(), "other", "yes", "7");
        }
        finishSeed(bulk, A, B);
        Outcome result = verify(ordinary(), expected);
        assertEquals(2L, result.status.candidateSearchRequests());
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

    /** A fourth stage must reject partial or reversed chains and preserve the latest valid competing predecessor. */
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
                List.of("replace", "replace-A", "replace-B", "replace-new-C", "replace-D")
            )
        );
    }

    /** A real millis MAX value must remain usable at a full page's missing-format sentinel and document tie. */
    public void testOrdinaryMaximumRawTieContinuation() throws Exception {
        assertBoundaryPages(false, Long.MAX_VALUE - 1, Long.MAX_VALUE, true);
    }

    /** MAX-adjacent formatted strings are nonnull, but numeric continuation must avoid their parser domain. */
    public void testAdjacentOrdinaryMaximumRawContinuation() throws Exception {
        assertBoundaryPages(false, Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, true);
    }

    /** Negative adjacent extrema exercise a nonnull extended-year continuation and an omitted derived lower bound. */
    public void testAdjacentOrdinaryMinimumRawContinuation() throws Exception {
        assertBoundaryPages(false, Long.MIN_VALUE + 1, Long.MIN_VALUE + 2, false);
    }

    /** Nanos MAX must preserve its last fractional nanosecond and continue more than256 equal-time events. */
    public void testNanosMaximumRawTieContinuation() throws Exception {
        assertBoundaryPages(true, Long.MAX_VALUE - 1, Long.MAX_VALUE, true);
    }

    /** Timestamp materialization must preserve both declared types and actual extreme instants for either PRIMARY. */
    public void testDemandedExtremeTimestampsKeepDeclaredTypes() throws Exception {
        for (boolean nanos : List.of(false, true)) {
            create(nanos ? "date_nanos" : "date", nanos ? null : "epoch_millis", 1, A, B);
            try {
                Instant first = nanos
                    ? org.elasticsearch.common.time.DateUtils.MAX_NANOSECOND_INSTANT.minusNanos(1)
                    : Instant.ofEpochMilli(Long.MAX_VALUE - 1);
                Instant second = nanos
                    ? org.elasticsearch.common.time.DateUtils.MAX_NANOSECOND_INSTANT
                    : Instant.ofEpochMilli(Long.MAX_VALUE);
                var bulk = client().prepareBulk();
                add(bulk, A, "A", nanos ? first.toString() : Long.MAX_VALUE - 1, "key", "yes", "7");
                add(bulk, B, "B", nanos ? second.toString() : Long.MAX_VALUE, "key", "yes", "7");
                finishSeed(bulk, A, B);
                assertRawMappedPages(A, nanos, Long.MAX_VALUE - 1, List.of("A"));
                assertRawMappedPages(B, nanos, Long.MAX_VALUE, List.of("B"));
                DataType type = nanos ? DataType.DATE_NANOS : DataType.DATETIME;
                for (int primary : List.of(1, 2)) {
                    BoundaryOutcome result = observeBoundary(boundaryQuery(primary, nanos ? "2nanos" : "2ms", true));
                    assertTypedBoundary(result.outcome, primary, first, second, type, type);
                }
            } finally {
                cleanup();
            }
        }
    }

    /** Requested raw millis and nanos units must preserve both one-nanosecond boundaries across stage mappings. */
    public void testRawMixedTimestampUnitsPreserveSubMillisOrdering() throws Exception {
        for (boolean nanosFirst : List.of(false, true)) {
            create(nanosFirst ? "date_nanos" : "date", 1, A);
            create(nanosFirst ? "date" : "date_nanos", 1, B);
            try {
                Instant first = nanosFirst ? BASE.minusNanos(1) : BASE;
                Instant second = nanosFirst ? BASE : BASE.plusNanos(1);
                var bulk = client().prepareBulk();
                add(bulk, A, "A", first.toString(), "key", "yes", "7");
                add(bulk, B, "B", second.toString(), "key", "yes", "7");
                add(bulk, A, "outside-A", nanosFirst ? BASE.minusNanos(2).toString() : BASE.toString(), "outside", "yes", "7");
                add(bulk, B, "outside-B", nanosFirst ? BASE.toString() : BASE.plusNanos(2).toString(), "outside", "yes", "7");
                finishSeed(bulk, A, B);
                for (int primary : List.of(1, 2)) {
                    BoundaryOutcome result = observeBoundary(boundaryQuery(primary, "1nanos", true));
                    assertTypedBoundary(
                        result.outcome,
                        primary,
                        first,
                        second,
                        nanosFirst ? DataType.DATE_NANOS : DataType.DATETIME,
                        nanosFirst ? DataType.DATETIME : DataType.DATE_NANOS
                    );
                }
            } finally {
                cleanup();
            }
        }
    }

    /** Record true MIN indexing admission separately; only an admitted field can establish native MIN paging support. */
    public void testOrdinaryMinimumAdmissionObservation() throws Exception {
        create("date", "epoch_millis", 1, A, B);
        try {
            try {
                prepareIndex(A).setId("minimum-admission")
                    .setSource(Map.of("@timestamp", Long.MIN_VALUE, "key", "probe", "disposition", "NO", "payload", "7"))
                    .get();
                refresh(A);
            } catch (Exception failure) {
                String error = errorChain(failure);
                assertTrue(
                    "unrelated indexing errors are not a domain observation: " + error,
                    error.contains(Long.toString(Long.MIN_VALUE)) && error.toLowerCase(java.util.Locale.ROOT).contains("parse")
                );
                System.out.println("TEMPORAL_MIN_ADMISSION supported=false; native MIN paging was NOT executed; error=" + error);
                return;
            }
            System.out.println("TEMPORAL_MIN_ADMISSION supported=true; native MIN paging follows");
            assertSeededBoundaryPages(false, Long.MIN_VALUE, Long.MIN_VALUE + 1, false);
        } finally {
            cleanup();
        }
    }

    private void assertBoundaryPages(boolean nanos, long first, long second, boolean extraEarlyStart) throws Exception {
        create(nanos ? "date_nanos" : "date", nanos ? null : "epoch_millis", 1, A, B);
        try {
            assertSeededBoundaryPages(nanos, first, second, extraEarlyStart);
        } finally {
            cleanup();
        }
    }

    private void assertSeededBoundaryPages(boolean nanos, long first, long second, boolean extraEarlyStart) throws Exception {
        var bulk = client().prepareBulk();
        List<String> firstIds = new ArrayList<>(), secondIds = new ArrayList<>();
        if (extraEarlyStart) {
            add(bulk, A, "unused-A", nanos ? Instant.EPOCH.toString() : 0L, "unused", "yes", "7");
            firstIds.add("unused-A");
        }
        for (int row = 0; row < 257; row++) {
            String key = "boundary-" + (1000 + row);
            String aId = "A-" + row, bId = "B-" + row;
            Object aTime = nanos ? Instant.ofEpochSecond(0, first).toString() : first;
            Object bTime = nanos ? Instant.ofEpochSecond(0, second).toString() : second;
            add(bulk, A, aId, aTime, key, "yes", "7");
            add(bulk, B, bId, bTime, key, "yes", "7");
            firstIds.add(aId);
            secondIds.add(bId);
        }
        finishSeed(bulk, A, B);
        assertRawMappedPages(A, nanos, first, firstIds);
        assertRawMappedPages(B, nanos, second, secondIds);
        for (int primary : List.of(1, 2)) {
            BoundaryOutcome result = observeBoundary(boundaryQuery(primary, nanos ? "2nanos" : "2ms", false));
            List<List<Object>> expected = new ArrayList<>();
            for (int row = 0; row < 257; row++) {
                expected.add(List.of("boundary-" + (1000 + row), (primary == 1 ? "A-" : "B-") + row, (primary == 1 ? "B-" : "A-") + row));
            }
            assertRows(expected, result.outcome);
            assertEquals(extraEarlyStart ? 515L : 514L, result.outcome.status.candidateRowsFetched());
            assertEquals(4L, result.outcome.status.candidateSearchRequests());
            assertNumericContinuations(result, A, first, nanos);
            assertNumericContinuations(result, B, second, nanos);
            if (nanos == false && first < org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_MINUS_9999) {
                for (CandidateRequest request : result.requests) {
                    if (request.indices.contains(B)) assertFalse(request.query, request.query.contains("\"gte\""));
                }
            }
        }
    }

    private static String boundaryQuery(int primary, String span, boolean timestamps) {
        int other = primary == 1 ? 2 : 1;
        String projection = "key,_id" + (timestamps ? ",@timestamp" : "");
        return "SEQUENCE BY key WITHIN "
            + span
            + " PRIMARY "
            + primary
            + " (FROM "
            + A
            + " METADATA _id | WHERE disposition==\"yes\" | KEEP "
            + projection
            + "),"
            + " (FROM "
            + B
            + " METADATA _id | WHERE disposition==\"yes\" | KEEP "
            + projection
            + ")"
            + " | EVAL other_id=MEMBER("
            + other
            + ",_id)"
            + (timestamps ? ",other_timestamp=MEMBER(" + other + ",@timestamp)" : "")
            + " | KEEP key,_id,other_id"
            + (timestamps ? ",@timestamp,other_timestamp" : "")
            + " | SORT key | LIMIT 1000";
    }

    private static void assertTypedBoundary(
        Outcome outcome,
        int primary,
        Instant first,
        Instant second,
        DataType firstType,
        DataType secondType
    ) {
        assertOk(outcome);
        assertEquals(List.of("key", "_id", "other_id", "@timestamp", "other_timestamp"), outcome.columnNames);
        assertEquals(
            List.of(
                DataType.KEYWORD,
                DataType.KEYWORD,
                DataType.KEYWORD,
                primary == 1 ? firstType : secondType,
                primary == 1 ? secondType : firstType
            ),
            outcome.columnTypes
        );
        assertEquals(1, outcome.rows.size());
        List<Object> row = outcome.rows.getFirst();
        assertEquals(List.of("key", primary == 1 ? "A" : "B", primary == 1 ? "B" : "A"), row.subList(0, 3));
        assertEquals(primary == 1 ? first : second, Instant.parse((String) row.get(3)));
        assertEquals(primary == 1 ? second : first, Instant.parse((String) row.get(4)));
    }

    private BoundaryOutcome observeBoundary(String query) throws Exception {
        try (var capture = new CandidateRequests()) {
            Outcome outcome = observe(syncEsqlQueryRequest(query).profile(true), 1000);
            return new BoundaryOutcome(outcome, List.copyOf(capture.requests));
        }
    }

    private static void assertNumericContinuations(BoundaryOutcome result, String index, long timestamp, boolean nanos) {
        List<CandidateRequest> pages = result.requests.stream()
            .filter(r -> r.indices.contains(index) && r.after.isEmpty() == false)
            .toList();
        assertEquals("each raw stream must continue beyond256 hits", 1, pages.size());
        CandidateRequest page = pages.getFirst();
        assertEquals(nanos ? "date_nanos" : "date", page.numericType);
        assertEquals(2, page.after.size());
        assertEquals(Long.class, page.after.get(0).getClass());
        assertEquals(timestamp, page.after.get(0));
        assertEquals(Long.class, page.after.get(1).getClass());
        assertTrue(page.query, page.query.contains(SequenceRepeatabilityQueryBuilder.NAME));
    }

    /** Own an ordinary control PIT and prove both raw comparator cells and exact multi-page source identities before SEQUENCE. */
    private void assertRawMappedPages(String index, boolean nanos, long timestamp, List<String> expectedIds) {
        BytesReference pit = new BytesArray(
            BytesRef.deepCopyOf(
                client().execute(
                    TransportOpenPointInTimeAction.TYPE,
                    new OpenPointInTimeRequest(index).keepAlive(TimeValue.timeValueMinutes(2)).allowPartialSearchResults(false)
                ).actionGet().getPointInTimeId().toBytesRef()
            )
        );
        Throwable primary = null;
        try {
            List<String> ids = new ArrayList<>();
            java.util.Set<String> unique = new java.util.HashSet<>();
            Object[] after = null, sampleFormatted = null, sampleRaw = null;
            Long previousTime = null, previousDoc = null;
            int pages = 0;
            while (true) {
                assertTrue("bounded ordinary raw continuation must finish within three requests", pages < 3);
                var source = new SearchSourceBuilder().query(org.elasticsearch.index.query.QueryBuilders.termQuery("disposition", "yes"))
                    .size(256)
                    .fetchSource(false)
                    .trackTotalHits(false)
                    .sort(
                        new FieldSortBuilder("@timestamp").order(SortOrder.ASC)
                            .setNumericType(nanos ? "date_nanos" : "date")
                            .unmappedType(nanos ? "date_nanos" : "date")
                            .setFormat("strict_date_optional_time_nanos")
                    )
                    .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC))
                    .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2)));
                if (after != null) source.searchAfter(after);
                var request = new SearchRequest().source(source).allowPartialSearchResults(false);
                request.setPreFilterShardSize(Integer.MAX_VALUE);
                var response = client().search(request).actionGet();
                int count;
                try {
                    if (response.pointInTimeId() != null) pit = new BytesArray(BytesRef.deepCopyOf(response.pointInTimeId().toBytesRef()));
                    assertFalse(response.isTimedOut());
                    assertEquals(0, response.getFailedShards());
                    var hits = response.getHits().getHits();
                    count = hits.length;
                    pages++;
                    for (var hit : hits) {
                        Object[] raw = hit.getRawSortValues(), formatted = hit.getSortValues();
                        assertEquals(2, raw.length);
                        assertEquals(Long.class, raw[0].getClass());
                        assertEquals(Long.class, raw[1].getClass());
                        long time = (Long) raw[0], doc = (Long) raw[1];
                        if (previousTime != null) {
                            assertTrue(
                                "ordinary comparator tuple must advance strictly",
                                time > previousTime || (time == previousTime && doc > previousDoc)
                            );
                        }
                        previousTime = time;
                        previousDoc = doc;
                        assertTrue("ordinary continuation must not duplicate a source identity", unique.add(hit.getId()));
                        if (((Long) raw[0]) == timestamp) {
                            sampleRaw = raw.clone();
                            sampleFormatted = formatted.clone();
                            if (timestamp == Long.MIN_VALUE || timestamp == Long.MAX_VALUE) assertNull(formatted[0]);
                            else assertNotNull(formatted[0]);
                        }
                        ids.add(hit.getId());
                        assertTrue("ordinary raw control cannot exceed its exact seeded row cap", ids.size() <= expectedIds.size());
                    }
                    if (count > 0) after = hits[count - 1].getRawSortValues().clone();
                } finally {
                    response.decRef();
                }
                if (count < 256) break;
            }
            assertNotNull(sampleRaw);
            assertNotNull(sampleFormatted);
            assertEquals(expectedIds.size(), ids.size());
            assertEquals(new java.util.HashSet<>(expectedIds), new java.util.HashSet<>(ids));
            assertEquals(expectedIds.size() > 256 ? 2 : 1, pages);
            System.out.println(
                "RAW_TEMPORAL_CONTROL index="
                    + index
                    + " numeric_type="
                    + (nanos ? "date_nanos" : "date")
                    + " formatted="
                    + Arrays.toString(sampleFormatted)
                    + " raw="
                    + Arrays.toString(sampleRaw)
                    + " pages="
                    + pages
                    + " rows="
                    + ids.size()
            );
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                assertTrue(
                    client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded()
                );
            } catch (Exception | Error cleanup) {
                if (primary != null) {
                    if (primary != cleanup) primary.addSuppressed(cleanup);
                } else throw cleanup;
            }
        }
    }

    private record BoundaryOutcome(Outcome outcome, List<CandidateRequest> requests) {}

    private record CandidateRequest(List<String> indices, String query, List<Object> after, String numericType) {}

    private static final class CandidateRequests implements AutoCloseable {
        private static final java.util.concurrent.atomic.AtomicReference<CandidateRequests> ACTIVE =
            new java.util.concurrent.atomic.AtomicReference<>();
        private final List<CandidateRequest> requests = new java.util.concurrent.CopyOnWriteArrayList<>();

        private CandidateRequests() {
            assertTrue(ACTIVE.compareAndSet(null, this));
        }

        @Override
        public void close() {
            ACTIVE.compareAndSet(this, null);
        }
    }

    /** A fully selected unread start can exclude an all-rejected later tail before any matcher prefix exists. */
    public void testUpperBoundSkipsRejectedTailBeforeMatchingStarts() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.toString(), "key", "yes", "7");
        for (int event = 0; event < 1536; event++) {
            add(bulk, B, "tail-" + event, BASE.plusSeconds(61).plusMillis(event).toString(), "key", "NO", "7");
        }
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence("60s", stage(A), branch(B)));
        assertRows(List.of(), result.outcome);
        assertEquals(
            "the unreachable tail must be excluded before its first256-row page",
            1L,
            result.outcome.status.candidateRowsFetched()
        );
        assertEquals(2L, result.outcome.status.candidateSearchRequests());
        assertUpperRequests(result, B, BASE.plusSeconds(60));
    }

    /** Fully read start input still has a future survivor after the earlier pair has consumed every pending prefix. */
    public void testUpperBoundIncludesEveryUnreadStart() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "early-A", BASE.toString(), "early", "yes", "7");
        add(bulk, A, "late-A", BASE.plusMillis(1000).toString(), "late", "yes", "7");
        add(bulk, B, "early-B", BASE.plusMillis(1).toString(), "early", "yes", "7");
        for (int event = 0; event < 255; event++) {
            add(bulk, B, "other-" + event, BASE.plusMillis(2 + event).toString(), "other-" + event, "yes", "7");
        }
        add(bulk, B, "late-B", BASE.plusMillis(1005).toString(), "late", "yes", "7");
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence("10ms", stage(A), stage(B)));
        assertRows(List.of(List.of("early", "early-A", "early-B"), List.of("late", "late-A", "late-B")), result.outcome);
        assertTrue(
            "the late completion must require a later candidate page",
            result.requests.stream().anyMatch(r -> r.indices.contains(B) && r.after.isEmpty() == false)
        );
        assertUpperRequests(result, B, BASE.plusMillis(1010));
    }

    /** A full first page is not EOF; unknown future starts forbid a finite upper range on a later reader. */
    public void testUpperBoundWaitsForRawStageZeroEOF() throws Exception {
        create("date", 1, A, B);
        var bulk = client().prepareBulk();
        for (int event = 0; event < 256; event++) {
            add(bulk, A, "early-" + event, BASE.plusMillis(event).toString(), "early-" + event, "yes", "7");
        }
        add(bulk, A, "late-A", BASE.plusMillis(1000).toString(), "late", "yes", "7");
        add(bulk, B, "late-B", BASE.plusMillis(1005).toString(), "late", "yes", "7");
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence("10ms", stage(A), stage(B)));
        assertRows(List.of(List.of("late", "late-A", "late-B")), result.outcome);
        assertEquals(List.of(), timestampUppers(firstRequest(result, B).query));
    }

    /** A prefix that advanced to any incomplete middle stage retains its original start as the inclusive horizon. */
    public void testUpperBoundIncludesIntermediateOnlyPrefixes() throws Exception {
        for (int stages : List.of(3, 4)) {
            create("date", 1, stages == 3 ? new String[] { A, B, C } : new String[] { A, B, C, D });
            try {
                var bulk = client().prepareBulk();
                add(bulk, A, "A", BASE.plusMillis(100).toString(), "key", "yes", "7");
                add(bulk, B, "B", BASE.plusMillis(105).toString(), "key", "yes", "7");
                if (stages == 4) {
                    add(bulk, C, "C", BASE.plusMillis(106).toString(), "key", "yes", "7");
                }
                String terminal = stages == 3 ? C : D;
                for (int event = 0; event < 256; event++) {
                    add(
                        bulk,
                        terminal,
                        "other-" + event,
                        BASE.plusMillis(stages == 3 ? 106 : 107).plusNanos(event * 1000L).toString(),
                        "other-" + event,
                        "yes",
                        "7"
                    );
                }
                add(bulk, terminal, "terminal", BASE.plusMillis(110).toString(), "key", "yes", "7");
                finishSeed(bulk, stages == 3 ? new String[] { A, B, C } : new String[] { A, B, C, D });
                UpperOutcome result = observeUpper(
                    stages == 3
                        ? upperSequence("10ms", stage(A), stage(B), stage(C))
                        : upperSequence("10ms", stage(A), stage(B), stage(C), stage(D))
                );
                assertRows(
                    stages == 3 ? List.of(List.of("key", "A", "B", "terminal")) : List.of(List.of("key", "A", "B", "C", "terminal")),
                    result.outcome
                );
                List<CandidateRequest> reloaded = result.requests.stream()
                    .filter(r -> r.indices.contains(terminal) && r.after.isEmpty() == false)
                    .toList();
                assertFalse("the terminal event must be on a later page", reloaded.isEmpty());
                for (CandidateRequest request : reloaded) {
                    assertEquals(List.of(BASE.plusMillis(110).toString()), timestampUppers(request.query));
                }
            } finally {
                cleanup();
            }
        }
    }

    /** Every start branch participates, including empty/fully rejected branches, in either declared order. */
    public void testUpperBoundIncludesAllStartAndLaterBranches() throws Exception {
        create("date", 1, A, B, C, D, E, F);
        var bulk = client().prepareBulk();
        add(bulk, A, "early-A", BASE.toString(), "early", "yes", "7");
        add(bulk, C, "late-C", BASE.plusMillis(1000).toString(), "late", "yes", "7");
        add(bulk, E, "filtered-E", BASE.plusMillis(500000).toString(), "filtered", "NO", "7");
        add(bulk, B, "early-B", BASE.plusMillis(1).toString(), "early", "yes", "7");
        add(bulk, B, "late-B", BASE.plusMillis(1005).toString(), "late", "yes", "7");
        for (int event = 0; event < 512; event++) {
            add(bulk, F, "filtered-F" + event, BASE.plusMillis(100000 + event).toString(), "late", "NO", "7");
        }
        finishSeed(bulk, A, B, C, D, E, F);
        for (boolean reverse : List.of(false, true)) {
            String starts = reverse
                ? "from (" + branch(E) + "), (" + branch(D) + "), (" + branch(C) + "), (" + branch(A) + ")"
                : "from (" + branch(A) + "), (" + branch(C) + "), (" + branch(D) + "), (" + branch(E) + ")";
            String later = "from (" + stage(B) + "), (" + branch(F) + ")";
            UpperOutcome result = observeUpper(upperSequence("10ms", starts, later));
            assertRows(List.of(List.of("early", "early-A", "early-B"), List.of("late", "late-C", "late-B")), result.outcome);
            assertUpperRequests(result, B, BASE.plusMillis(1010));
            assertUpperRequests(result, F, BASE.plusMillis(1010));
        }
    }

    /** An unknown future page in any start branch forbids U globally, even if another branch already reached EOF. */
    public void testUpperBoundWaitsForEveryStartBranchEOF() throws Exception {
        create("date", 1, A, B, C, D);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", BASE.toString(), "ignored", "yes", "7");
        for (int event = 0; event < 256; event++) {
            add(bulk, C, "early-" + event, BASE.plusMillis(event + 1).toString(), "early-" + event, "yes", "7");
        }
        add(bulk, C, "late-C", BASE.plusMillis(1000).toString(), "late", "yes", "7");
        add(bulk, B, "late-B", BASE.plusMillis(1005).toString(), "late", "yes", "7");
        finishSeed(bulk, A, B, C, D);
        for (boolean reverse : List.of(false, true)) {
            String starts = reverse
                ? "from (" + stage(D) + "), (" + stage(C) + "), (" + stage(A) + ")"
                : "from (" + stage(A) + "), (" + stage(C) + "), (" + stage(D) + ")";
            UpperOutcome result = observeUpper(upperSequence("10ms", starts, stage(B)));
            assertRows(List.of(List.of("late", "late-C", "late-B")), result.outcome);
            assertEquals(List.of(), timestampUppers(firstRequest(result, B).query));
        }
    }

    /** Inclusive millis and nanos bounds exclude only the next representable instant, including fractional carry. */
    public void testUpperBoundIsInclusiveAcrossTimestampPrecisions() throws Exception {
        for (boolean nanos : List.of(false, true)) {
            create(nanos ? "date_nanos" : "date", 1, A, B);
            try {
                Instant start = nanos ? BASE.plusNanos(999999) : BASE.plusMillis(1000);
                Instant end = nanos ? start.plusNanos(2) : start.plusMillis(2);
                var bulk = client().prepareBulk();
                add(bulk, A, "inside-A", start.toString(), "inside", "yes", "7");
                add(bulk, A, "outside-A", start.toString(), "outside", "yes", "7");
                add(bulk, B, "inside-B", end.toString(), "inside", "yes", "7");
                add(bulk, B, "outside-B", (nanos ? end.plusNanos(1) : end.plusMillis(1)).toString(), "outside", "yes", "7");
                finishSeed(bulk, A, B);
                UpperOutcome result = observeUpper(upperSequence(nanos ? "2nanos" : "2ms", stage(A), stage(B)));
                assertRows(List.of(List.of("inside", "inside-A", "inside-B")), result.outcome);
                assertEquals(3L, result.outcome.status.candidateRowsFetched());
                assertUpperRequests(result, B, end);
            } finally {
                cleanup();
            }
        }
    }

    /** Ordinary epoch-millis addition may overflow its long domain; a populated target must remain readable. */
    public void testUpperBoundOmitsOrSaturatesOrdinaryMillisOverflow() throws Exception {
        create("date", "epoch_millis", 1, A, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", Long.MAX_VALUE - 1, "key", "yes", "7");
        add(bulk, B, "B", Long.MAX_VALUE, "key", "yes", "7");
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence("2ms", stage(A), stage(B)));
        assertRows(List.of(List.of("key", "A", "B")), result.outcome);
        for (String upper : timestampUppers(firstRequest(result, B).query)) {
            assertEquals(Instant.ofEpochMilli(Long.MAX_VALUE), Instant.parse(upper));
        }
    }

    /** Ordinary dates beyond the nanos range must never be converted to an epoch-nanos long. */
    public void testUpperBoundSupportsOrdinaryDateBeyondNanosDomain() throws Exception {
        create("date", 1, A, B);
        Instant start = Instant.parse("2500-01-01T00:00:00Z");
        var bulk = client().prepareBulk();
        add(bulk, A, "A", start.toString(), "key", "yes", "7");
        add(bulk, B, "B", start.plusMillis(1).toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence("2ms", stage(A), stage(B)));
        assertRows(List.of(List.of("key", "A", "B")), result.outcome);
        assertUpperRequests(result, B, start.plusMillis(2));
    }

    /** An upper limit above the nanos maximum is unconstrained, rather than match-none or an invalid parsed bound. */
    public void testUpperBoundAboveNanosMaximumKeepsLastRepresentableEvent() throws Exception {
        create("date_nanos", 1, A, B);
        Instant maximum = org.elasticsearch.common.time.DateUtils.MAX_NANOSECOND_INSTANT;
        var bulk = client().prepareBulk();
        add(bulk, A, "A", maximum.minusNanos(1).toString(), "key", "yes", "7");
        add(bulk, B, "B", maximum.toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence("2nanos", stage(A), stage(B)));
        assertRows(List.of(List.of("key", "A", "B")), result.outcome);
        for (String upper : timestampUppers(firstRequest(result, B).query)) {
            assertEquals(maximum, Instant.parse(upper));
        }
    }

    /** Nanos-domain rejection uses the sum U+span; a negative sum cannot reach a populated epoch target. */
    public void testUpperBoundBelowNanosEpochIsEmpty() throws Exception {
        verifyPreEpochUpperBound(-2, "1ms", false);
    }

    /** A negative ordinary-date start can still reach the nanos domain when adding span crosses the epoch. */
    public void testUpperBoundCrossingNanosEpochKeepsMatch() throws Exception {
        verifyPreEpochUpperBound(-1, "2ms", true);
    }

    private void verifyPreEpochUpperBound(long startMillis, String span, boolean matches) throws Exception {
        create("date", "epoch_millis", 1, A);
        create("date_nanos", 1, B);
        var bulk = client().prepareBulk();
        add(bulk, A, "A", startMillis, "key", "yes", "7");
        add(bulk, B, "B", Instant.EPOCH.toString(), "key", "yes", "7");
        finishSeed(bulk, A, B);
        UpperOutcome result = observeUpper(upperSequence(span, stage(A), stage(B)));
        assertRows(matches ? List.of(List.of("key", "A", "B")) : List.of(), result.outcome);
        if (matches) {
            assertUpperRequests(result, B, Instant.ofEpochMilli(1));
        } else {
            assertEquals(1L, result.outcome.status.candidateRowsFetched());
            assertNotNull("the guarded populated target still needs its ordinary scoped search", firstRequest(result, B));
            for (CandidateRequest request : result.requests) {
                if (request.indices.contains(B)) {
                    assertTrue("an out-of-domain upper bound must not be parsed as nanos", request.query.contains("match_none"));
                    assertTrue(
                        "match-none must retain repeatability validation",
                        request.query.contains(SequenceRepeatabilityQueryBuilder.NAME)
                    );
                }
            }
        }
    }

    private UpperOutcome observeUpper(String query) throws Exception {
        try (var capture = new CandidateRequests()) {
            // Existing observe(String) has an outer BASE time filter; that could mask the generated U or its domain errors.
            Outcome outcome = observe(syncEsqlQueryRequest(query).profile(true));
            return new UpperOutcome(outcome, List.copyOf(capture.requests));
        }
    }

    private static String upperSequence(String span, String... sources) {
        StringBuilder query = new StringBuilder("SEQUENCE BY key WITHIN ").append(span).append(" PRIMARY 1 ");
        for (int stage = 0; stage < sources.length; stage++) {
            if (stage != 0) query.append(", ");
            query.append("(").append(sources[stage]).append(")");
        }
        for (int stage = 1; stage < sources.length; stage++) {
            query.append(" | EVAL member").append(stage + 1).append("_id=MEMBER(").append(stage + 1).append(",_id)");
        }
        query.append(" | KEEP key,_id");
        for (int stage = 1; stage < sources.length; stage++)
            query.append(",member").append(stage + 1).append("_id");
        return query.append(" | SORT key | LIMIT 100").toString();
    }

    private static CandidateRequest firstRequest(UpperOutcome outcome, String index) {
        return outcome.requests.stream().filter(r -> r.indices.contains(index)).findFirst().orElseThrow();
    }

    private static void assertUpperRequests(UpperOutcome outcome, String index, Instant expected) {
        List<CandidateRequest> requests = outcome.requests.stream().filter(r -> r.indices.contains(index)).toList();
        assertFalse("expected actual candidate request for [" + index + "]", requests.isEmpty());
        for (CandidateRequest request : requests) {
            assertEquals(request.query, List.of(expected.toString()), timestampUppers(request.query));
        }
    }

    private static List<String> timestampUppers(String query) {
        List<String> values = new ArrayList<>();
        collectTimestampUppers(
            org.elasticsearch.common.xcontent.XContentHelper.convertToMap(
                org.elasticsearch.xcontent.json.JsonXContent.jsonXContent,
                query,
                false
            ),
            values
        );
        return List.copyOf(values);
    }

    private static void collectTimestampUppers(Object value, List<String> values) {
        if (value instanceof Map<?, ?> map) {
            if (map.get("range") instanceof Map<?, ?> fields && fields.get("@timestamp") instanceof Map<?, ?> range) {
                assertFalse("generated upper timestamp limits must be inclusive", range.containsKey("lt"));
                if (range.get("lte") != null) values.add(range.get("lte").toString());
            }
            map.values().forEach(child -> collectTimestampUppers(child, values));
        } else if (value instanceof List<?> list) {
            list.forEach(child -> collectTimestampUppers(child, values));
        }
    }

    private record UpperOutcome(Outcome outcome, List<CandidateRequest> requests) {}

    private static String chain(String... indices) {
        StringBuilder query = new StringBuilder("sequence by key within 60s PRIMARY 1 ");
        for (int i = 0; i < indices.length; i++) {
            if (i > 0) query.append(", ");
            query.append("(").append(stage(indices[i])).append(")");
        }
        for (int i = 1; i < indices.length; i++) {
            query.append(" | EVAL member").append(i + 1).append("_id = MEMBER(").append(i + 1).append(", _id)");
        }
        query.append(" | keep key, _id");
        for (int i = 1; i < indices.length; i++) {
            query.append(", member").append(i + 1).append("_id");
        }
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
        return observe(request, 100);
    }

    private Outcome observe(EsqlQueryRequest request, int outputCap) throws Exception {
        String coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        var future = new PlainActionFuture<Outcome>();
        try {
            client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
                try {
                    assertFalse(response.isPartial());
                    List<List<Object>> rows = getValuesList(response);
                    assertTrue("output cap reached", rows.size() < outputCap);
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
                    future.onResponse(
                        new Outcome(
                            rows,
                            status,
                            warnings,
                            "",
                            response.columns().stream().map(column -> column.name()).toList(),
                            response.columns().stream().map(column -> column.type()).toList()
                        )
                    );
                } catch (Exception e) {
                    future.onFailure(e);
                } catch (AssertionError e) {
                    future.onFailure(new IllegalStateException(e));
                }
            }, e -> future.onResponse(new Outcome(List.of(), null, List.of(), errorChain(e), List.of(), List.of()))));
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
        return "sequence by key within 60s PRIMARY 1 ("
            + first
            + "), ("
            + second
            + ") | EVAL member2_id = MEMBER(2, _id) | keep key, _id, member2_id | sort key | limit 100";
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
        for (String index : List.of(A, B, C, D, E, F)) {
            if (indexExists(index)) assertAcked(indicesAdmin().prepareDelete(index));
        }
    }

    private record Outcome(
        List<List<Object>> rows,
        SequenceSourceStatus status,
        List<String> warnings,
        String error,
        List<String> columnNames,
        List<DataType> columnTypes
    ) {}
}
