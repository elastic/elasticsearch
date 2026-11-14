/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * A fingerprint of the documents the exact search matched (count and sum of sequence numbers), used to tell later if a ground
 * truth went out of date.
 * <p>
 * Sequence numbers only increase within a shard, so the pair cannot stay equal there when documents change; across shards it
 * can (a 4 deleted in one, a 4 added in another). So the state is read per shard and the digest includes the shard. A copy that
 * is behind can show an old state. Above 2^53 a shard's sum is inexact, so only its first digits are taken.
 *
 * @param documents the number of documents that were matched
 * @param digest    a hash of the document count and sequence number sum of every shard of every index
 */
public record DataState(long documents, long digest) {

    static final String BY_INDEX = "by_index";
    static final String SEQ_NO_SUM = "seq_no_sum";
    static final String SHARD_PREFERENCE = "_shards:";

    // more shards than this in one index is taken for a failure to find the end of them
    private static final int MAX_SHARDS = 10_000;
    private static final int MAX_INDICES = 10_000;
    private static final long SEED = 0x71d5ca3b7e4f1a29L;

    // above this a double is no longer exact for whole numbers
    private static final double EXACT_LIMIT = 9_007_199_254_740_992.0;
    private static final int SIGNIFICANT_BITS = 40;

    private record ShardState(String index, int shard, long documents, double seqNoSum) {}

    public static boolean isProbe(SearchRequest request) {
        return request.preference() != null && request.preference().startsWith(SHARD_PREFERENCE);
    }

    /**
     * Counts and sums the sequence numbers of the matched documents per index on one shard number, without a scan.
     * {@link SearchResponse#getTotalShards()} is 0 when no index has that shard number.
     */
    static SearchRequest probe(CapturedQuery query, int shard) {
        BoolQueryBuilder matched = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(query.field()));
        for (QueryBuilder filter : query.filters()) {
            matched.filter(filter);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(matched)
            .size(0)
            .trackTotalHits(false)
            .aggregation(
                AggregationBuilders.terms(BY_INDEX)
                    .field(IndexFieldMapper.NAME)
                    .size(MAX_INDICES)
                    .subAggregation(AggregationBuilders.sum(SEQ_NO_SUM).field(SeqNoFieldMapper.NAME))
            );
        // a probe that leaves out the shards that failed would count fewer documents, and could still equal an incomplete state
        return new SearchRequest(query.indices()).source(source).preference(SHARD_PREFERENCE + shard).allowPartialSearchResults(false);
    }

    /**
     * Reads the state shard number by shard number through {@code search}, so it runs with the identity of the caller.
     */
    public static void read(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search,
        CapturedQuery query,
        ActionListener<DataState> listener
    ) {
        read(search, query, 0, new ArrayList<>(), listener);
    }

    private static void read(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search,
        CapturedQuery query,
        int shard,
        List<ShardState> states,
        ActionListener<DataState> listener
    ) {
        if (shard >= MAX_SHARDS) {
            listener.onFailure(new IllegalStateException("found more than [" + MAX_SHARDS + "] shards in an index"));
            return;
        }
        // a search function may complete the listener and then still throw, only the first outcome counts
        ActionListener<SearchResponse> done = ActionListener.notifyOnce(ActionListener.wrap(response -> {
            if (add(response, shard, states)) {
                read(search, query, shard + 1, states, listener);
            } else {
                listener.onResponse(of(states));
            }
        }, listener::onFailure));
        try {
            search.accept(probe(query, shard), done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }

    private static boolean add(SearchResponse response, int shard, List<ShardState> states) {
        if (response.getTotalShards() == 0) {
            return false;
        }
        Terms byIndex = response.getAggregations() == null ? null : response.getAggregations().get(BY_INDEX);
        if (byIndex == null) {
            throw new IllegalStateException("the search did not tell the state of the data");
        }
        if (byIndex.getSumOfOtherDocCounts() > 0) {
            throw new IllegalStateException("found more than [" + MAX_INDICES + "] indices to take the state of");
        }
        for (Terms.Bucket bucket : byIndex.getBuckets()) {
            Sum sum = bucket.getAggregations().get(SEQ_NO_SUM);
            states.add(new ShardState(bucket.getKeyAsString(), shard, bucket.getDocCount(), sum.value()));
        }
        return true;
    }

    private static DataState of(List<ShardState> states) {
        states.sort(Comparator.comparing(ShardState::index).thenComparingInt(ShardState::shard));
        long documents = 0;
        byte[][] indices = new byte[states.size()][];
        int size = 0;
        for (int i = 0; i < indices.length; i++) {
            indices[i] = states.get(i).index().getBytes(StandardCharsets.UTF_8);
            size += Integer.BYTES + indices[i].length + Integer.BYTES + Long.BYTES + Long.BYTES;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < indices.length; i++) {
            ShardState state = states.get(i);
            documents += state.documents();
            buffer.putInt(indices[i].length)
                .put(indices[i])
                .putInt(state.shard())
                .putLong(state.documents())
                .putLong(rounded(state.seqNoSum()));
        }
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(buffer.array(), 0, size, SEED, new MurmurHash3.Hash128());
        return new DataState(documents, hash.h1);
    }

    private static long rounded(double sum) {
        if (Math.abs(sum) <= EXACT_LIMIT) {
            return (long) sum;
        }
        int dropped = Math.getExponent(sum) - SIGNIFICANT_BITS;
        return (long) (sum / Math.scalb(1.0, dropped)) ^ dropped;
    }
}
