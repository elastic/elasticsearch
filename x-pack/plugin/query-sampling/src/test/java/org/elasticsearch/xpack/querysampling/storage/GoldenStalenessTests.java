/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GoldenStalenessTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private final List<SearchRequest> goldenSearches = new ArrayList<>();
    private final List<SearchRequest> probes = new ArrayList<>();

    private List<SearchHit> records = List.of();
    private Long latestCompleted = 3L; // null: there is no golden index

    /**
     * What the data is like now, for the query whose vector starts with a number: the state that it answers to the probe of it.
     */
    private Function<Float, DataState> now = x -> new DataState(5, 10.0);

    private GoldenStaleness staleness(BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe) {
        GoldenReader reader = new GoldenReader((request, listener) -> {
            goldenSearches.add(request);
            if (latestCompleted == null) {
                listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
            } else if (request.source().size() == 1) {
                respond(listener, latestCompleted == 0 ? List.of() : List.of(manifest(latestCompleted)), null);
            } else {
                respond(listener, records, null);
            }
        }, REGISTRY);
        return new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            probe.accept(request, listener);
        });
    }

    private GoldenStaleness staleness() {
        return staleness((request, listener) -> { respond(listener, List.of(), now.apply(firstOf(request))); });
    }

    /**
     * The first component of the vector of the query that is probed, which the fake gets from its index expression: the
     * query of the record {@code n} is searched in the index {@code "idx" + n}.
     */
    private static float firstOf(SearchRequest request) {
        return Float.parseFloat(request.indices()[0].substring(3));
    }

    private GoldenStaleness.Result check(GoldenStaleness staleness, long version, int max) {
        AtomicReference<GoldenStaleness.Result> result = new AtomicReference<>();
        staleness.check(version, max, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    public void testQueriesWhoseDataIsAsItWasAreFreshAndThoseWhoseDataChangedAreStale() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)), record(2, new DataState(5, 10.0)), record(3, new DataState(5, 9.0)));
        now = x -> new DataState(5, 10.0);

        GoldenStaleness.Result result = check(staleness(), 3, 10);

        assertThat(result, equalTo(new GoldenStaleness.Result(3, 3, 2, 1, 0, 0)));
    }

    public void testDocumentsThatWereAddedOrDeletedMakeTheGroundTruthStale() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)), record(2, new DataState(5, 10.0)));
        now = x -> x == 1 ? new DataState(6, 10.0) : new DataState(4, 10.0);

        assertThat(check(staleness(), 3, 10), equalTo(new GoldenStaleness.Result(3, 2, 0, 2, 0, 0)));
    }

    public void testGroundTruthWithoutTheStateOfTheDataCannotBeTold() throws IOException {
        records = List.of(record(1, null), record(2, new DataState(5, 10.0)));

        GoldenStaleness.Result result = check(staleness(), 3, 10);

        assertThat(result, equalTo(new GoldenStaleness.Result(3, 2, 1, 0, 1, 0)));
        assertThat("there is nothing to compare, so no search is made for it", probes.size(), equalTo(1));
    }

    public void testEachQueryIsLookedAtWithAProbeOfItsOwn() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)), record(2, new DataState(5, 10.0)));

        check(staleness(), 3, 10);

        assertThat(probes.stream().map(request -> request.indices()[0]).toList(), equalTo(List.of("idx1", "idx2")));
        assertThat(probes.get(0).source().size(), equalTo(0));
    }

    public void testWithoutAVersionTheLatestOneThatIsCompleteIsChecked() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)));
        latestCompleted = 7L;

        GoldenStaleness.Result result = check(staleness(), 0, 10);

        assertThat(result.version(), equalTo(7L));
        assertThat(
            "the version that was found is the one read",
            goldenSearches.get(1).source().query().toString().contains("\"value\" : 7"),
            equalTo(true)
        );
        assertThat(goldenSearches.get(1).source().size(), equalTo(10));
    }

    public void testAVersionThatIsAskedForIsNotLookedUp() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)));

        check(staleness(), 2, 10);

        assertThat("it is read at once", goldenSearches.size(), equalTo(1));
        assertThat(goldenSearches.get(0).source().query().toString().contains("\"value\" : 2"), equalTo(true));
    }

    public void testNothingIsCheckedWhenNothingWasPromoted() {
        latestCompleted = 0L;
        assertThat(check(staleness(), 0, 10), equalTo(new GoldenStaleness.Result(0, 0, 0, 0, 0, 0)));

        latestCompleted = null;
        assertThat("no golden index", check(staleness(), 0, 10), equalTo(new GoldenStaleness.Result(0, 0, 0, 0, 0, 0)));
        assertThat(probes.size(), equalTo(0));
    }

    public void testAFailedProbeIsCountedAsFailedAndDoesNotStopTheOthers() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)), record(2, new DataState(5, 10.0)), record(3, new DataState(5, 10.0)));
        GoldenStaleness staleness = staleness((request, listener) -> {
            if (firstOf(request) == 2f) {
                listener.onFailure(new IllegalStateException("no"));
            } else {
                respond(listener, List.of(), new DataState(5, 10.0));
            }
        });

        assertThat(check(staleness, 3, 10), equalTo(new GoldenStaleness.Result(3, 3, 2, 0, 0, 1)));
    }

    public void testAProbeThatThrowsAndAResponseWithoutAStateAreFailures() throws IOException {
        records = List.of(record(1, new DataState(5, 10.0)), record(2, new DataState(5, 10.0)));
        GoldenStaleness staleness = staleness((request, listener) -> {
            if (firstOf(request) == 1f) {
                throw new IllegalStateException("no");
            }
            respond(listener, List.of(), null);
        });

        assertThat(check(staleness, 3, 10), equalTo(new GoldenStaleness.Result(3, 2, 0, 0, 0, 2)));
    }

    public void testUnreadableRecordsAreCountedAsFailed() throws IOException {
        SearchHit broken = SearchHit.unpooled(9, "broken");
        broken.sourceRef(new BytesArray("{\"kind\":\"record\"}"));
        records = List.of(record(1, new DataState(5, 10.0)), broken);

        assertThat(check(staleness(), 3, 10), equalTo(new GoldenStaleness.Result(3, 2, 1, 0, 0, 1)));
    }

    public void testAFailedReadIsReported() {
        GoldenStaleness failing = new GoldenStaleness(
            new GoldenReader((request, listener) -> listener.onFailure(new IllegalStateException("read failed")), REGISTRY),
            (request, listener) -> fail("nothing to look at")
        );
        AtomicReference<Exception> failure = new AtomicReference<>();

        failing.check(3, 10, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalStateException.class));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    /**
     * A record of a golden version, of the query whose vector starts with {@code n}, which is searched in the index {@code idx<n>}.
     */
    private static SearchHit record(long n, DataState state) throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        StoredSample sample = new StoredSample(
            "sampler",
            String.valueOf(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0, 1.0),
            1L,
            2L,
            new GroundTruth(List.of(new CapturedSearch.Hit("idx" + n, "d" + n, 1f)), state),
            null,
            null,
            null,
            null
        );
        SearchHit hit = SearchHit.unpooled((int) n, GoldenRecord.recordId(3, sample.fingerprint()));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 3, sample, 9L)));
        }
        return hit;
    }

    /**
     * Answers as the transport does: the response is released once the listener is done with it.
     */
    private static void respond(ActionListener<SearchResponse> listener, List<SearchHit> hits, DataState state) {
        SearchHits searchHits = new SearchHits(
            hits.toArray(SearchHit[]::new),
            new TotalHits(state == null ? hits.size() : state.documents(), TotalHits.Relation.EQUAL_TO),
            1f
        );
        SearchResponse response = state == null
            ? SearchResponseUtils.successfulResponse(searchHits)
            : SearchResponseUtils.response(searchHits)
                .aggregations(InternalAggregations.from(List.of(new Sum("seq_no_sum", state.seqNoSum(), DocValueFormat.RAW, Map.of()))))
                .build();
        searchHits.decRef(); // the response holds its own reference
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }
}
