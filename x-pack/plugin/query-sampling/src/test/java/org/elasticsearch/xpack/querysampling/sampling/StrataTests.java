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
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StrataTests extends ESTestCase {

    private static SpatialStrata balancing(int clusters, int warmup, double balance) {
        SpatialStrata strata = new SpatialStrata(clusters, warmup, () -> new Random(7L));
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), balance).build(),
                Set.of(QuerySamplingSettings.SPATIAL_BALANCE)
            )
        );
        return strata;
    }

    private static AtomicReference<Stratum> assign(SpatialStrata strata, String field, float... vector) {
        AtomicReference<Stratum> told = new AtomicReference<>();
        strata.assign(field, vector, told::set);
        return told;
    }

    private static Stratum[] denseAndSparse(SpatialStrata strata, int dense) {
        AtomicReference<Stratum> first = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> sparse = assign(strata, "vector", 50, 50);
        AtomicReference<Stratum> last = first;
        for (int i = 1; i < dense; i++) {
            last = assign(strata, "vector", 0, 0.1f);
        }
        return new Stratum[] { last.get(), sparse.get() };
    }

    public void testQueriesWaitForTheClustersAndAreToldAllAtOnce() {
        SpatialStrata strata = balancing(2, 4, 0.5);

        AtomicReference<Stratum> first = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> second = assign(strata, "vector", 100, 100);
        AtomicReference<Stratum> third = assign(strata, "vector", 0, 1);
        assertThat("not told yet", first.get(), nullValue());
        assertThat(second.get(), nullValue());
        assertThat(strata.counts("vector/2").length, equalTo(0));

        AtomicReference<Stratum> fourth = assign(strata, "vector", 101, 100);

        assertThat(first.get(), equalTo(third.get()));
        assertThat(second.get(), equalTo(fourth.get()));
        assertThat(first.get(), not(equalTo(second.get())));
        assertThat(strata.counts("vector/2"), equalTo(new long[] { 2, 2 }));
    }

    public void testGroupsThatAreFarApartAreInClustersOfTheirOwn() {
        int[] sizes = { 100, 50, 40, 10 };
        SpatialStrata strata = balancing(4, 200, 0.5);
        Random random = new Random(randomLong());
        List<List<AtomicReference<Stratum>>> told = new ArrayList<>();
        List<Integer> arrivals = new ArrayList<>();
        for (int group = 0; group < sizes.length; group++) {
            told.add(new ArrayList<>());
            for (int i = 0; i < sizes[group]; i++) {
                arrivals.add(group);
            }
        }
        Collections.shuffle(arrivals, random);
        for (int group : arrivals) {
            told.get(group).add(assign(strata, "vector", 100f * group + random.nextFloat(), random.nextFloat(), random.nextFloat()));
        }

        long[] counts = strata.counts("vector/3");
        Arrays.sort(counts);
        assertThat(counts, equalTo(new long[] { 10, 40, 50, 100 }));
        for (int group = 0; group < sizes.length; group++) {
            Set<Stratum> clusters = new HashSet<>();
            told.get(group).forEach(reference -> clusters.add(reference.get()));
            assertThat("group " + group + " is in one cluster", clusters.size(), equalTo(1));
        }
    }

    public void testNewQueriesJoinTheNearestClusterAndMoveItsCentroid() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        AtomicReference<Stratum> a = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> b = assign(strata, "vector", 100, 0);

        assertThat(assign(strata, "vector", 8, 0).get(), equalTo(a.get()));

        assertThat("47 from (4, 0), where it is 51 from (0, 0)", assign(strata, "vector", 51, 0).get(), equalTo(a.get()));
        assertThat(assign(strata, "vector", 99, 1).get(), equalTo(b.get()));
        assertThat(Arrays.stream(strata.counts("vector/2")).sum(), equalTo(5L));
    }

    public void testSparseClustersAreFavouredAndDenseOnesAreNot() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        Stratum[] found = denseAndSparse(strata, 9);

        assertThat(strata.factor(found[0]), closeTo(5.0 / 9, 1e-9));
        assertThat(strata.factor(found[1]), closeTo(5.0, 1e-9));
    }

    public void testFieldsAndDimensionsAreSeparateSpaces() {
        SpatialStrata strata = balancing(1, 1, 1.0);

        Stratum a = assign(strata, "a", 1, 2).get();
        Stratum b = assign(strata, "b", 1, 2).get();
        Stratum longer = assign(strata, "a", 1, 2, 3).get();

        assertThat(a.space(), equalTo("a/2"));
        assertThat(b.space(), equalTo("b/2"));
        assertThat(longer.space(), equalTo("a/3"));
        assertThat(strata.counts("a/2"), equalTo(new long[] { 1 }));
    }

    public void testVectorsThatAreNotFiniteAreLeftOut() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        assign(strata, "vector", 1, 1);

        assertThat(assign(strata, "vector", Float.NaN, 1).get(), nullValue());
        assertThat(assign(strata, "vector", 1, Float.POSITIVE_INFINITY).get(), nullValue());
        assertThat("they do not count towards the clusters either", strata.counts("vector/2").length, equalTo(0));
        assertNotNull(assign(strata, "vector", 3, 3).get());
        assertThat(strata.counts("vector/2"), equalTo(new long[] { 1, 1 }));
    }

    private static List<CapturedSearch.Hit> hits(float... scores) {
        return IntStream.range(0, scores.length).mapToObj(i -> new CapturedSearch.Hit("idx", "doc" + i, scores[i])).toList();
    }

    private static List<CapturedSearch.Hit> hitsWithContrast(double contrast) {
        float others = (float) ((3 * contrast - 1) / 2);
        return hits(1f, others, others);
    }

    private static HardnessStrata tilting(double tilt) {
        HardnessStrata strata = new HardnessStrata();
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.HARDNESS_TILT.getKey(), tilt).build(),
                Set.of(QuerySamplingSettings.HARDNESS_TILT)
            )
        );
        return strata;
    }

    private static void learn(HardnessStrata strata, String field, int queries) {
        for (int i = 0; i < queries; i++) {
            strata.assign(field, 2, hitsWithContrast(0.4 + 0.5 * i / queries));
        }
    }

    public void testContrastIsOneWhenAllHitsAreAsGoodAsTheFirstAndSmallerWhenTheyAreNot() {
        assertThat(HardnessStrata.contrast(hits(1f, 1f, 1f)), closeTo(1.0, 1e-9));
        assertThat(HardnessStrata.contrast(hits(1f, 0.5f, 0.25f)), closeTo(1.75 / 3, 1e-9));
        assertThat("it is the best that counts, not the first", HardnessStrata.contrast(hits(0.5f, 1f, 0.25f)), closeTo(1.75 / 3, 1e-9));
    }

    public void testThereIsNoContrastWithoutAReasonableAnswer() {
        assertTrue(Double.isNaN(HardnessStrata.contrast(List.of())));
        assertTrue("too few hits", Double.isNaN(HardnessStrata.contrast(hits(1f, 0.5f))));
        assertTrue("a score of zero", Double.isNaN(HardnessStrata.contrast(hits(1f, 0.5f, 0f))));
        assertTrue("a negative score", Double.isNaN(HardnessStrata.contrast(hits(1f, 0.5f, -1f))));
        assertTrue("not a number", Double.isNaN(HardnessStrata.contrast(hits(1f, Float.NaN, 0.5f))));
    }

    public void testQueriesAreMediumUntilThereAreEnoughToTellThemApart() {
        HardnessStrata strata = tilting(1.0);

        for (int i = 0; i < HardnessStrata.MIN_QUERIES; i++) {
            assertThat(strata.assign("vec", 2, hitsWithContrast(0.4 + 0.5 * i / HardnessStrata.MIN_QUERIES)), equalTo(Hardness.MEDIUM));
        }
    }

    public void testQueriesAreToldApartByHowFarTheyAreFromTheOthers() {
        HardnessStrata strata = tilting(1.0);
        learn(strata, "vec", 100);

        assertThat(strata.assign("vec", 2, hitsWithContrast(0.95)), equalTo(Hardness.HARD));
        assertThat(strata.assign("vec", 2, hitsWithContrast(0.35)), equalTo(Hardness.EASY));
        assertThat(strata.assign("vec", 2, hitsWithContrast(0.65)), equalTo(Hardness.MEDIUM));
    }

    public void testAboutAThirdOfTheQueriesAreInEachBucket() {
        HardnessStrata strata = tilting(1.0);
        Random random = new Random(randomLong());
        int queries = 600;
        int[] counts = new int[Hardness.values().length];
        for (int i = 0; i < 500 + queries; i++) {
            double contrast = Math.min(1.0, Math.max(0.34, 0.65 + 0.1 * random.nextGaussian()));
            Hardness hardness = strata.assign("vec", 2, hitsWithContrast(contrast));
            if (i >= 500) {
                counts[hardness.ordinal()]++;
            }
        }

        for (int count : counts) {
            assertThat((double) count / queries, closeTo(1.0 / 3, 0.08));
        }
    }

    public void hardnessStrataTestFieldsAndDimensionsAreSeparateSpaces() {
        HardnessStrata strata = tilting(1.0);
        learn(strata, "a", 100);

        assertThat("it knows nothing of this field", strata.assign("b", 2, hitsWithContrast(0.95)), equalTo(Hardness.MEDIUM));
        assertThat("nor of these dimensions", strata.assign("a", 3, hitsWithContrast(0.95)), equalTo(Hardness.MEDIUM));
        assertThat(strata.assign("a", 2, hitsWithContrast(0.95)), equalTo(Hardness.HARD));
    }

    public void testHardQueriesAreFavouredAndEasyOnesAreNot() {
        HardnessStrata strata = tilting(1.0);

        assertThat(strata.factor(Hardness.HARD), greaterThan(1.0));
        assertThat(strata.factor(Hardness.EASY), lessThan(1.0));
        assertThat(strata.factor(Hardness.HARD) / strata.factor(Hardness.MEDIUM), closeTo(Math.E, 1e-9));
        assertThat(strata.factor(Hardness.MEDIUM) / strata.factor(Hardness.EASY), closeTo(Math.E, 1e-9));
    }

    public void testAThirdOfEachGivesAnAverageOfOne() {
        for (double tilt : new double[] { 0.1, 1.0, 3.0, 10.0 }) {
            HardnessStrata strata = tilting(tilt);
            double sum = 0;
            for (Hardness hardness : Hardness.values()) {
                sum += strata.factor(hardness);
            }
            assertThat("tilt " + tilt, sum / 3, closeTo(1.0, 1e-9));
        }
    }

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

    private void answer(SearchRequest request, ActionListener<SearchResponse> listener) {
        boolean all = request.source().query() instanceof ExistsQueryBuilder;
        long total = all ? VECTORS : passing;
        respond(listener, total);
    }

    private static void respond(ActionListener<SearchResponse> listener, long total) {
        SearchHits hits = SearchHits.empty(new TotalHits(total, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(hits);
        hits.decRef();
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

    public void testNoVectorsAtAllIsAFailure() {
        SelectivityStrata strata = strata(true);
        TrackedQuery tracked = tracked(1);
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
}
