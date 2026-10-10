/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SelectivityStrataTests extends ESTestCase {

    private static final long VECTORS = 1000;

    private final AtomicLong now = new AtomicLong();
    private final List<SearchRequest> searches = new ArrayList<>();
    private final List<ActionListener<SearchResponse>> held = new ArrayList<>();
    private final MultiplicityTracker tracker = new MultiplicityTracker(1000);
    private long passing = 100;
    private boolean answerRightAway = true;

    private SelectivityStrata strata(boolean enabled) {
        SelectivityStrata strata = new SelectivityStrata((request, listener) -> {
            searches.add(request);
            if (answerRightAway) {
                answer(request, listener);
            } else {
                held.add(listener);
            }
        }, now::get);
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.ESTIMATE_SELECTIVITY.getKey(), enabled).build(),
                Set.of(QuerySamplingSettings.ESTIMATE_SELECTIVITY)
            )
        );
        return strata;
    }

    /**
     * The vectors that there are when asked for all of them, and those that pass the filters when asked for those.
     */
    private void answer(SearchRequest request, ActionListener<SearchResponse> listener) {
        boolean all = request.source().query() instanceof ExistsQueryBuilder;
        long total = all ? VECTORS : passing;
        respond(listener, total);
    }

    /**
     * Answers as the transport does: the response is released once the listener is done with it.
     */
    private static void respond(ActionListener<SearchResponse> listener, long total) {
        SearchHits hits = SearchHits.empty(new TotalHits(total, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(hits);
        hits.decRef(); // the response holds its own reference
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    private static CapturedQuery filtered(long id) {
        return new CapturedQuery(
            new String[] { "idx" },
            "vec",
            new float[] { id },
            10,
            100,
            null,
            null,
            List.of(QueryBuilders.termQuery("category", id)),
            null
        );
    }

    private static CapturedQuery unfiltered() {
        return new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
    }

    private TrackedQuery tracked(long id) {
        return tracker.record(new QueryFingerprint(id, id));
    }

    public void testQueriesWithoutFiltersAreUnfilteredWithoutAnySearchAndWhateverTheSetting() {
        SelectivityStrata strata = strata(false);
        TrackedQuery tracked = tracked(1);

        strata.assign(tracked, unfiltered());

        assertThat(tracked.selectivity(), equalTo(Selectivity.UNFILTERED));
        assertThat(searches.size(), equalTo(0));
    }

    public void testNothingIsCountedUnlessTheSettingAsksForIt() {
        SelectivityStrata strata = strata(false);
        TrackedQuery tracked = tracked(1);

        strata.assign(tracked, filtered(1));

        assertThat(tracked.selectivity(), nullValue());
        assertThat(searches.size(), equalTo(0));
    }

    public void testTheShareOfTheVectorsThatPassTheFiltersTellsTheSelectivity() {
        SelectivityStrata strata = strata(true);
        long[][] cases = {
            { 600, Selectivity.HIGH.ordinal() },
            { 500, Selectivity.HIGH.ordinal() },
            { 499, Selectivity.MEDIUM.ordinal() },
            { 50, Selectivity.MEDIUM.ordinal() },
            { 49, Selectivity.LOW.ordinal() },
            { 0, Selectivity.LOW.ordinal() } };
        long id = 1;
        for (long[] oneCase : cases) {
            passing = oneCase[0];
            TrackedQuery tracked = tracked(id);

            strata.assign(tracked, filtered(id++));

            assertThat("passing " + oneCase[0], tracked.selectivity(), equalTo(Selectivity.values()[(int) oneCase[1]]));
        }
        assertThat(strata.counted(), equalTo((long) cases.length));
    }

    public void testTheCountsAskForTheVectorsThatPassAndForAllOfThem() {
        SelectivityStrata strata = strata(true);

        strata.assign(tracked(1), filtered(1));

        assertThat("one for the vectors there are, one for those that pass", searches.size(), equalTo(2));
        for (SearchRequest request : searches) {
            assertThat("only the count is wanted", request.source().size(), equalTo(0));
            assertThat(request.source().trackTotalHitsUpTo(), equalTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE));
            assertThat(request.indices(), equalTo(new String[] { "idx" }));
        }
        assertTrue(searches.get(0).source().query() instanceof ExistsQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) searches.get(1).source().query();
        assertThat("the vector has to be there, and the filter of the query", bool.filter().size(), equalTo(2));
    }

    public void testTheNumberOfVectorsIsKnownForAWhileAndCountedAgainAfterwards() {
        SelectivityStrata strata = strata(true);

        strata.assign(tracked(1), filtered(1));
        strata.assign(tracked(2), filtered(2));
        assertThat("the second only counts what passes", searches.size(), equalTo(3));

        now.addAndGet(SelectivityStrata.TOTAL_VALID_MILLIS);
        strata.assign(tracked(3), filtered(3));
        assertThat("and after a while it is counted again", searches.size(), equalTo(5));
    }

    public void testTheShareIsNeverMoreThanOne() {
        SelectivityStrata strata = strata(true);
        passing = VECTORS + 100; // the documents changed between the counts
        TrackedQuery tracked = tracked(1);

        strata.assign(tracked, filtered(1));

        assertThat(tracked.selectivity(), equalTo(Selectivity.HIGH));
    }

    public void testNotMoreThanAFewCountsAtATimeAndTheRestAreSkipped() {
        answerRightAway = false;
        SelectivityStrata strata = strata(true);
        List<TrackedQuery> queries = new ArrayList<>();
        for (int i = 0; i < SelectivityStrata.MAX_IN_FLIGHT + 2; i++) {
            queries.add(tracked(i));
            strata.assign(queries.get(i), filtered(i));
        }

        assertThat(searches.size(), equalTo(SelectivityStrata.MAX_IN_FLIGHT));
        assertThat(strata.skipped(), equalTo(2L));
        assertThat(queries.get(SelectivityStrata.MAX_IN_FLIGHT).selectivity(), nullValue());

        // one finishes, which makes room for the next
        answerRightAway = true;
        ActionListener<SearchResponse> first = held.remove(0);
        answer(searches.get(0), first);
        strata.assign(tracked(100), filtered(100));
        assertThat("the room that was made is used", strata.skipped(), equalTo(2L));
    }

    public void testAFailedCountLeavesTheSelectivityUnknownAndFreesTheRoom() {
        SelectivityStrata failing = new SelectivityStrata(
            (request, listener) -> listener.onFailure(new IllegalStateException("no")),
            now::get
        );
        failing.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.ESTIMATE_SELECTIVITY.getKey(), true).build(),
                Set.of(QuerySamplingSettings.ESTIMATE_SELECTIVITY)
            )
        );
        for (int i = 0; i < SelectivityStrata.MAX_IN_FLIGHT * 3; i++) {
            TrackedQuery tracked = tracked(i);
            failing.assign(tracked, filtered(i));
            assertThat(tracked.selectivity(), nullValue());
        }

        assertThat(failing.failed(), equalTo((long) SelectivityStrata.MAX_IN_FLIGHT * 3));
        assertThat("a failure does not hold the room", failing.skipped(), equalTo(0L));
    }

    public void testSearchThatThrowsCountsAsFailed() {
        SelectivityStrata throwing = new SelectivityStrata((request, listener) -> { throw new IllegalStateException("no"); }, now::get);
        throwing.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.ESTIMATE_SELECTIVITY.getKey(), true).build(),
                Set.of(QuerySamplingSettings.ESTIMATE_SELECTIVITY)
            )
        );

        throwing.assign(tracked(1), filtered(1));

        assertThat(throwing.failed(), equalTo(1L));
    }

    public void testNoVectorsAtAllIsAFailure() {
        SelectivityStrata strata = strata(true);
        TrackedQuery tracked = tracked(1);
        // an index with no vectors answers a count of 0 for all of them
        SelectivityStrata empty = new SelectivityStrata((request, listener) -> respond(listener, 0), now::get);
        empty.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.ESTIMATE_SELECTIVITY.getKey(), true).build(),
                Set.of(QuerySamplingSettings.ESTIMATE_SELECTIVITY)
            )
        );

        empty.assign(tracked, filtered(1));

        assertThat(tracked.selectivity(), nullValue());
        assertThat(empty.failed(), equalTo(1L));
        assertThat(strata.counted(), equalTo(0L));
    }

    public void testSettingFollowsWhenItChanges() {
        SelectivityStrata strata = new SelectivityStrata((request, listener) -> answer(request, listener), now::get);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.ESTIMATE_SELECTIVITY));
        strata.watch(clusterSettings);
        TrackedQuery before = tracked(1);
        strata.assign(before, filtered(1));
        assertThat(before.selectivity(), nullValue());

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.ESTIMATE_SELECTIVITY.getKey(), true).build());
        TrackedQuery after = tracked(2);
        strata.assign(after, filtered(2));

        assertThat(after.selectivity(), equalTo(Selectivity.MEDIUM));
    }

    public void testTheBoundariesOfTheSelectivities() {
        assertThat(Selectivity.of(1.0), equalTo(Selectivity.HIGH));
        assertThat(Selectivity.of(0.5), equalTo(Selectivity.HIGH));
        assertThat(Selectivity.of(0.4999), equalTo(Selectivity.MEDIUM));
        assertThat(Selectivity.of(0.05), equalTo(Selectivity.MEDIUM));
        assertThat(Selectivity.of(0.0499), equalTo(Selectivity.LOW));
        assertThat(Selectivity.of(0.0), equalTo(Selectivity.LOW));
    }
}
