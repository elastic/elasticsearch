/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Answers the searches that read the {@link DataState} as the search does, for shards that are made up: one shard number at a time,
 * with the documents it matched and the sum of their sequence numbers for each index. It is the one thing that is not a real class
 * here, as the answer of the search is what the real shards give and a unit test has none.
 */
public final class FakeShards implements BiConsumer<SearchRequest, ActionListener<SearchResponse>> {

    /**
     * @param documents the documents of the shard that the search matches
     * @param seqNoSum  the sum of their sequence numbers
     */
    public record Shard(String index, int number, long documents, long seqNoSum) {}

    private final List<Shard> shards;

    private FakeShards(List<Shard> shards) {
        this.shards = shards;
    }

    public static FakeShards of(Shard... shards) {
        return new FakeShards(List.of(shards));
    }

    /**
     * Whether the request is one that reads the state of the data.
     */
    public static boolean isProbe(SearchRequest request) {
        return DataState.isProbe(request);
    }

    /**
     * The state that is read from these shards.
     */
    public DataState state() {
        AtomicReference<DataState> state = new AtomicReference<>();
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        DataState.read(this, query, ActionListener.wrap(state::set, e -> { throw new AssertionError(e); }));
        return state.get();
    }

    @Override
    public void accept(SearchRequest request, ActionListener<SearchResponse> listener) {
        int number = Integer.parseInt(request.preference().substring(DataState.SHARD_PREFERENCE.length()));
        List<Shard> numbered = shards.stream().filter(shard -> shard.number() == number).toList();
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (Shard shard : numbered) {
            if (shard.documents() > 0) {
                Sum sum = new Sum(DataState.SEQ_NO_SUM, shard.seqNoSum(), DocValueFormat.RAW, Map.of());
                buckets.add(
                    new StringTerms.Bucket(
                        new BytesRef(shard.index()),
                        shard.documents(),
                        InternalAggregations.from(List.of(sum)),
                        false,
                        0,
                        DocValueFormat.RAW
                    )
                );
            }
        }
        StringTerms byIndex = new StringTerms(
            DataState.BY_INDEX,
            BucketOrder.count(false),
            BucketOrder.count(false),
            10,
            1,
            Map.of(),
            DocValueFormat.RAW,
            10,
            false,
            0,
            buckets,
            0L
        );
        SearchResponse response = SearchResponseUtils.response()
            .aggregations(InternalAggregations.from(List.of(byIndex)))
            .shards(numbered.size(), numbered.size(), 0)
            .build();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }
}
