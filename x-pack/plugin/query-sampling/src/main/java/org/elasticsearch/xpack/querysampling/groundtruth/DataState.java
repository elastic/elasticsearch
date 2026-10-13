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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * What the data was like when a ground truth was computed, as far as that ground truth is concerned: the documents that
 * the exact search matched, which are those that have the vector and pass the filters of the query.
 * <p>
 * It is a fingerprint and not a copy. Every insert, update and delete gives what is stored a sequence number higher than any
 * its shard has given before, so for one shard the number of matched documents and the sum of their sequence numbers cannot
 * stay the same when they change. Each shard counts on its own, though, so for a whole index they can (a document with the
 * number 4 deleted from one shard and another with the number 4 added to a second). That is why the state is read for each
 * shard of each index, and the digest is of all of them with the shard they were read from. Each is read on any copy of the
 * shard, so a copy that is behind can show an old state until it has caught up. Above 2^53 the sum of a shard is not exact, so
 * only its first digits are taken, and a change of a single sequence number there is not seen.
 *
 * @param documents the number of documents that were matched
 * @param digest    a hash of the number of documents and the sum of their sequence numbers of every shard of every index
 */
public record DataState(long documents, long digest) {

    static final String BY_INDEX = "by_index";
    static final String SEQ_NO_SUM = "seq_no_sum";
    static final String SHARD_PREFERENCE = "_shards:";

    /**
     * More shards than this in one index is taken for a failure to find the end of them.
     */
    private static final int MAX_SHARDS = 10_000;
    private static final int MAX_INDICES = 10_000;
    private static final long SEED = 0x71d5ca3b7e4f1a29L;

    /**
     * Above this a double is no longer exact for whole numbers.
     */
    private static final double EXACT_LIMIT = 9_007_199_254_740_992.0;
    private static final int SIGNIFICANT_BITS = 40;

    private record ShardState(String index, int shard, long documents, double seqNoSum) {}

    /**
     * Whether the request is one of those that read the state of the data.
     */
    public static boolean isProbe(SearchRequest request) {
        return request.preference() != null && request.preference().startsWith(SHARD_PREFERENCE);
    }

    /**
     * A search that tells the state of the shards that have a number, without a scan: it only matches the documents that have the
     * vector and pass the filters, and counts them and sums their sequence numbers for each index. Which shards have the number is
     * told by {@link SearchResponse#getTotalShards()}, which is 0 when none has.
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
        return new SearchRequest(query.indices()).source(source).preference(SHARD_PREFERENCE + shard);
    }

    /**
     * Reads the state of the data that the exact search of a query is over, one shard number after the other until there are no
     * more, through the given function so that it is read with the identity of whoever asked for it.
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
        ActionListener<SearchResponse> done = new ActionListener<>() {
            // a search function may complete the listener and then still throw, only the first outcome counts
            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void onResponse(SearchResponse response) {
                boolean more;
                try {
                    more = add(response, shard, states);
                } catch (Exception e) {
                    onFailure(e);
                    return;
                }
                if (completed.compareAndSet(false, true) == false) {
                    return;
                }
                if (more) {
                    read(search, query, shard + 1, states, listener);
                } else {
                    listener.onResponse(of(states));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (completed.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
            }
        };
        try {
            search.accept(probe(query, shard), done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }

    /**
     * Adds what the response tells of the shards with the number, and returns whether there may be shards with a higher one.
     */
    private static boolean add(SearchResponse response, int shard, List<ShardState> states) {
        if (response.getFailedShards() > 0) {
            throw new IllegalStateException("the state of [" + response.getFailedShards() + "] shards could not be read");
        }
        if (response.getTotalShards() == 0) {
            return false;
        }
        Terms byIndex = response.getAggregations() == null ? null : response.getAggregations().get(BY_INDEX);
        if (byIndex == null) {
            throw new IllegalStateException("the search did not tell the state of the data");
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

    /**
     * The sum as a whole number, with only its first digits when it is too large to be exact.
     */
    private static long rounded(double sum) {
        if (Math.abs(sum) <= EXACT_LIMIT) {
            return (long) sum;
        }
        int dropped = Math.getExponent(sum) - SIGNIFICANT_BITS;
        return (long) (sum / Math.scalb(1.0, dropped)) ^ dropped;
    }
}
