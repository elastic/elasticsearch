/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Promotes stored sampled queries that have their ground truth to a new version of the golden dataset, which {@link GoldenWriter}
 * makes.
 * <p>
 * A query is in a version once, even if more than one run of the sampler picked it: the one that was picked last is taken.
 * Arrivals that were kept as events are not promoted, a golden query is a query and not one of its searches.
 */
public final class GoldenPromoter {

    /**
     * @param version  the version that was made, 0 if there was nothing to promote and none was made
     * @param promoted queries that are in the version
     * @param failed   queries that could not be written to it
     */
    public record Result(long version, int promoted, int failed) {}

    private final StoredSamples samples;
    private final GoldenWriter writer;

    /**
     * All of them are done as the plugin, as nobody else may touch these indices.
     *
     * @param search   searches the index of the sample and the golden dataset
     * @param registry needed to read the filters of the stored queries
     * @param clock    milliseconds since the epoch
     */
    public GoldenPromoter(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search,
        BiConsumer<IndexRequest, ActionListener<DocWriteResponse>> index,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk,
        BiConsumer<UpdateRequest, ActionListener<UpdateResponse>> update,
        NamedXContentRegistry registry,
        LongSupplier clock
    ) {
        this.samples = new StoredSamples(search, registry);
        this.writer = new GoldenWriter(search, index, bulk, update, clock);
    }

    /**
     * Promotes up to {@code max} of the stored queries that have ground truth, the most recently picked first.
     */
    public void promote(int max, ActionListener<Result> listener) {
        samples.read(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("has_ground_truth", true))
                .mustNot(QueryBuilders.existsQuery("event_id")),
            "picked_at",
            SortOrder.DESC,
            max,
            ActionListener.wrap(read -> promote(oncePerQuery(read.samples()), listener), listener::onFailure)
        );
    }

    /**
     * The most recent of the stored queries of each fingerprint, in the order they came.
     */
    private static List<StoredSample> oncePerQuery(List<StoredSample> stored) {
        Set<String> seen = new HashSet<>();
        List<StoredSample> once = new ArrayList<>();
        for (StoredSample sample : stored) {
            if (seen.add(sample.fingerprint())) {
                once.add(sample);
            }
        }
        return once;
    }

    private void promote(List<StoredSample> queries, ActionListener<Result> listener) {
        if (queries.isEmpty()) {
            listener.onResponse(new Result(0, 0, 0));
            return;
        }
        writer.write(queries, null, listener.map(written -> new Result(written.version(), written.written(), written.failed())));
    }
}
