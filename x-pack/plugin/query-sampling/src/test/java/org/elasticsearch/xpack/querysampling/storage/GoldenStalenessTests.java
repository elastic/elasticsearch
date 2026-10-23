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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.FakeShards;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class GoldenStalenessTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private static final FakeShards STORED_SHARDS = FakeShards.of(
        new FakeShards.Shard("idx", 0, 3, 6),
        new FakeShards.Shard("idx", 1, 2, 4)
    );

    private static final FakeShards CHANGED_SHARDS = FakeShards.of(
        new FakeShards.Shard("idx", 0, 3, 6),
        new FakeShards.Shard("idx", 1, 2, 9)
    );

    /** The shards of a state are read one number after the other, up to the first that none has: 0 and 1, and 2. */
    private static final int PROBES_PER_STATE = 3;

    private static final DataState STORED = STORED_SHARDS.state();

    private static final DataState CHANGED = CHANGED_SHARDS.state();

    private final List<SearchRequest> probes = new ArrayList<>();

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    private static StoredSample storedSample(long n, long pickedAt, String groundTruthId, DataState state) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        return new StoredSample(
            "sampler",
            fp(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0),
            pickedAt,
            pickedAt,
            new GroundTruth(List.of(new CapturedSearch.Hit("idx" + n, groundTruthId, 1f)), state),
            null,
            null,
            null,
            null
        );
    }

    private static String fp(long n) {
        return new QueryFingerprint(n, n).hex();
    }

    private static void respond(ActionListener<SearchResponse> listener, List<SearchHit> hits) {
        SearchHits searchHits = new SearchHits(hits.toArray(SearchHit[]::new), new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    private List<SearchHit> records = List.of();

    private GoldenStaleness staleness() {
        GoldenReader reader = new GoldenReader((request, listener) -> {
            if (request.source().size() == 1) {
                respond(listener, List.of(manifest(3)));
            } else {
                respond(listener, records);
            }
        }, REGISTRY);
        return new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            STORED_SHARDS.accept(request, listener);
        }, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    private GoldenStaleness.Result check(GoldenStaleness staleness, long version, int max) {
        AtomicReference<GoldenStaleness.Result> result = new AtomicReference<>();
        staleness.check(version, max, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    public void testQueriesWhoseDataIsAsItWasAreFreshAndThoseWhoseDataChangedAreStale() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED), record(3, CHANGED));

        GoldenStaleness.Result result = check(staleness(), 3, 10);

        assertThat(result, equalTo(new GoldenStaleness.Result(3, 3, 2, 1, 0, 0)));
    }

    public void testManyQueriesWithoutTheStateOfTheDataDoNotExhaustTheStack() {
        List<StoredSample> many = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            many.add(storedSample(1, 100, "d", null));
        }
        AtomicReference<List<GoldenStaleness.Status>> statuses = new AtomicReference<>();

        staleness().classify(many, ActionListener.wrap(statuses::set, e -> fail(e)));

        assertThat(statuses.get().size(), equalTo(many.size()));
        assertThat(Set.copyOf(statuses.get()), equalTo(Set.of(GoldenStaleness.Status.UNKNOWN)));
    }

    private static SearchHit record(long n, DataState state) throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        StoredSample sample = new StoredSample(
            "sampler",
            String.valueOf(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0),
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
}
