/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.util.Check;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.execution.search.Querier.closePointInTime;
import static org.elasticsearch.xpack.sql.execution.search.Querier.logSearchResponse;
import static org.elasticsearch.xpack.sql.execution.search.Querier.prepareRequest;

/**
 * Cursor for composite aggregation (GROUP BY).
 * Stores the query that gets updated/slides across requests.
 */
public class CompositeAggCursor implements Cursor {

    private static final Logger log = LogManager.getLogger(CompositeAggCursor.class);

    public static final String NAME = "c";

    private final String[] indices;
    private final SearchSourceBuilder nextQuery;
    private final List<BucketExtractor> extractors;
    private final BitSet mask;
    private final int limit;
    private final boolean includeFrozen;

    CompositeAggCursor(
        SearchSourceBuilder nextQuery,
        List<BucketExtractor> exts,
        BitSet mask,
        int remainingLimit,
        boolean includeFrozen,
        String... indices
    ) {
        this.indices = indices;
        this.nextQuery = nextQuery;
        this.extractors = exts;
        this.mask = mask;
        this.limit = remainingLimit;
        this.includeFrozen = includeFrozen;
    }

    public CompositeAggCursor(StreamInput in) throws IOException {
        indices = in.readStringArray();
        nextQuery = new SearchSourceBuilder(in);
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(BucketExtractor.class);
        mask = BitSet.valueOf(in.readByteArray());
        includeFrozen = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        nextQuery.writeTo(out);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
        out.writeByteArray(mask.toByteArray());
        out.writeBoolean(includeFrozen);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    String[] indices() {
        return indices;
    }

    SearchSourceBuilder next() {
        return nextQuery;
    }

    BitSet mask() {
        return mask;
    }

    List<BucketExtractor> extractors() {
        return extractors;
    }

    int limit() {
        return limit;
    }

    boolean includeFrozen() {
        return includeFrozen;
    }

    protected SearchSourceBuilder nextQuery() {
        return nextQuery;
    }

    @Override
    public void nextPage(SqlConfiguration cfg, Client client, ActionListener<Page> listener) {
        if (log.isTraceEnabled()) {
            log.trace("About to execute composite query {} on {}", StringUtils.toString(nextQuery), indices);
        }

        SearchRequest request = prepareRequest(nextQuery, cfg.requestTimeout(), includeFrozen, indices);

        client.search(request, new ActionListener.Delegating<>(listener) {
            @Override
            public void onResponse(SearchResponse response) {
                handle(
                    client,
                    response,
                    request.source(),
                    makeRowSet(response),
                    makeCursor(),
                    () -> client.search(request, this),
                    delegate,
                    couldProducePartialPages(getCompositeBuilder(next()))
                );
            }
        });
    }

    protected Supplier<CompositeAggRowSet> makeRowSet(SearchResponse response) {
        CompositeAggregationBuilder aggregation = getCompositeBuilder(nextQuery);
        return () -> new CompositeAggRowSet(extractors, mask, response, aggregation.size(), limit, couldProducePartialPages(aggregation));
    }

    protected BiFunction<SearchSourceBuilder, CompositeAggRowSet, CompositeAggCursor> makeCursor() {
        return (q, r) -> new CompositeAggCursor(q, r.extractors(), r.mask(), r.remainingData(), includeFrozen, indices);
    }

    static void handle(
        Client client,
        SearchResponse response,
        SearchSourceBuilder source,
        Supplier<CompositeAggRowSet> makeRowSet,
        BiFunction<SearchSourceBuilder, CompositeAggRowSet, CompositeAggCursor> makeCursor,
        Runnable retry,
        ActionListener<Page> listener,
        boolean couldProducePartialPages
    ) {
        if (log.isTraceEnabled()) {
            logSearchResponse(response, log);
        }

        // retry
        if (couldProducePartialPages && shouldRetryDueToEmptyPage(response)) {
            updateCompositeAfterKey(response, source);
            retry.run();
            return;
        }

        CompositeAggRowSet rowSet = makeRowSet.get();

        Map<String, Object> afterKey = rowSet.afterKey();

        if (afterKey != null) {
            updateSourceAfterKey(afterKey, source);
        }

        if (rowSet.remainingData() == 0) {
            closePointInTime(client, response.pointInTimeId(), listener.map(r -> Page.last(rowSet)));
        } else {
            listener.onResponse(new Page(rowSet, makeCursor.apply(source, rowSet)));
        }
    }

    private static boolean shouldRetryDueToEmptyPage(SearchResponse response) {
        CompositeAggregation composite = getComposite(response);
        // if there are no buckets but a next page, go fetch it instead of sending an empty response to the client
        return composite.getBuckets().isEmpty() && composite.afterKey() != null && composite.afterKey().isEmpty() == false;
    }

    static CompositeAggregationBuilder getCompositeBuilder(SearchSourceBuilder source) {
        AggregationBuilder aggregation = source.aggregations()
            .getAggregatorFactories()
            .stream()
            .filter(a -> Objects.equals(a.getName(), Aggs.ROOT_GROUP_NAME))
            .findFirst()
            .orElse(null);

        Check.isTrue(aggregation instanceof CompositeAggregationBuilder, "Unexpected aggregation builder " + aggregation);

        return (CompositeAggregationBuilder) aggregation;
    }

    static boolean couldProducePartialPages(CompositeAggregationBuilder aggregation) {
        for (var agg : aggregation.getPipelineAggregations()) {
            if (agg instanceof BucketSelectorPipelineAggregationBuilder) {
                return true;
            }
        }
        return false;
    }

    static CompositeAggregation getComposite(SearchResponse response) {
        Aggregation agg = response.getAggregations().get(Aggs.ROOT_GROUP_NAME);
        Check.isTrue(agg instanceof CompositeAggregation, "Unrecognized root group found; " + agg);

        return (CompositeAggregation) agg;
    }

    private static void updateCompositeAfterKey(SearchResponse r, SearchSourceBuilder search) {
        updateSourceAfterKey(getComposite(r).afterKey(), search);
    }

    private static void updateSourceAfterKey(Map<String, Object> afterKey, SearchSourceBuilder search) {
        AggregationBuilder aggBuilder = search.aggregations().getAggregatorFactories().iterator().next();
        // update after-key with the new value
        if (aggBuilder instanceof CompositeAggregationBuilder comp) {
            comp.aggregateAfter(afterKey);
        } else {
            throw new SqlIllegalArgumentException("Invalid client request; expected a group-by but instead got {}", aggBuilder);
        }
    }

    @Override
    public void clear(Client client, ActionListener<Boolean> listener) {
        Check.isTrue(nextQuery().pointInTimeBuilder() != null, "Expected cursor with point-in-time id but got null");
        closePointInTime(client, nextQuery().pointInTimeBuilder().getEncodedId(), listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), nextQuery, extractors, limit, mask, includeFrozen);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        CompositeAggCursor other = (CompositeAggCursor) obj;
        return Arrays.equals(indices, other.indices)
            && Objects.equals(nextQuery, other.nextQuery)
            && Objects.equals(extractors, other.extractors)
            && Objects.equals(limit, other.limit)
            && Objects.equals(includeFrozen, other.includeFrozen);
    }

    @Override
    public String toString() {
        return "cursor for composite on index [" + Arrays.toString(indices) + "]";
    }
}
