/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.GroundTruthRunner;
import org.elasticsearch.xpack.querysampling.storage.QuerySamplingIndex;
import org.elasticsearch.xpack.querysampling.storage.SampleRecord;
import org.elasticsearch.xpack.querysampling.storage.StoredSample;
import org.elasticsearch.xpack.querysampling.storage.StoredSamples;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Computes the ground truth of the sampled queries that are stored in {@link QuerySamplingIndex} and do not have
 * it yet, and stores it with them. It does not depend on which node picked a query, so any node can do it.
 * <p>
 * Two identities are involved, which is why there are two ways to search. The sample itself is read and updated
 * as the plugin, as nobody else may touch that index. The exact searches run over the data of the users, they are
 * done as whoever asked for the computation so that they only see what that person may see.
 */
public final class StoredGroundTruth {

    private static final Logger logger = LogManager.getLogger(StoredGroundTruth.class);

    private final StoredSamples samples;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> sampleBulk;
    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch;
    private final LongSupplier clock;

    /**
     * @param sampleSearch searches the index of the sample, as the plugin
     * @param sampleBulk   updates the index of the sample, as the plugin
     * @param exactSearch  runs the exact searches, as the caller
     * @param registry     needed to read the filters of the stored queries
     * @param clock        milliseconds since the epoch
     */
    public StoredGroundTruth(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> sampleSearch,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> sampleBulk,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch,
        NamedXContentRegistry registry,
        LongSupplier clock
    ) {
        this.samples = new StoredSamples(sampleSearch, registry);
        this.sampleBulk = sampleBulk;
        this.exactSearch = exactSearch;
        this.clock = clock;
    }

    /**
     * Computes the ground truth of up to {@code max} stored queries. The ones that were waiting longest go first.
     * A query whose computation fails stays without ground truth, but moves to the back of the line so that it
     * does not keep the others from being served.
     */
    public void compute(int max, ActionListener<GroundTruthRunner.Result> listener) {
        compute(max, null, listener);
    }

    /**
     * Like {@link #compute(int, ActionListener)}, but only for the stored queries that also match the filter, which is
     * how a node picks the ones it is responsible for.
     *
     * @param filter on the fields of the stored documents, or {@code null} for all that are pending
     */
    public void compute(int max, @Nullable QueryBuilder filter, ActionListener<GroundTruthRunner.Result> listener) {
        QueryBuilder pending = QueryBuilders.termQuery("has_ground_truth", false);
        samples.read(
            filter == null ? pending : QueryBuilders.boolQuery().filter(pending).filter(filter),
            "updated_at",
            SortOrder.ASC,
            max,
            ActionListener.wrap(read -> compute(read.samples(), read.unreadable(), listener), listener::onFailure)
        );
    }

    private void compute(List<StoredSample> samples, int unreadable, ActionListener<GroundTruthRunner.Result> listener) {
        Map<StoredSample, GroundTruth> computed = new IdentityHashMap<>();
        new GroundTruthRunner(exactSearch).run(
            samples,
            sample -> sample.search().query(),
            computed::put,
            ActionListener.wrap(result -> store(samples, computed, unreadable, listener), listener::onFailure)
        );
    }

    /**
     * Stores the ground truth of what was computed in one bulk request, which also moves what was not to the back
     * of the line.
     */
    private void store(
        List<StoredSample> samples,
        Map<StoredSample, GroundTruth> computed,
        int unreadable,
        ActionListener<GroundTruthRunner.Result> listener
    ) {
        BulkRequest request = new BulkRequest();
        long now = clock.getAsLong();
        for (StoredSample sample : samples) {
            GroundTruth groundTruth = computed.get(sample);
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                request.add(
                    new UpdateRequest(QuerySamplingIndex.NAME, sample.id()).retryOnConflict(3)
                        .doc(
                            groundTruth == null
                                ? SampleRecord.touch(builder, now)
                                : SampleRecord.groundTruthUpdate(builder, groundTruth, now)
                        )
                );
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }
        if (request.numberOfActions() == 0) {
            listener.onResponse(new GroundTruthRunner.Result(0, unreadable));
            return;
        }
        sampleBulk.accept(request, ActionListener.wrap(response -> {
            // the items are in the order of the request, which is the order of the samples
            int stored = 0;
            BulkItemResponse[] items = response.getItems();
            for (int i = 0; i < items.length; i++) {
                stored += items[i].isFailed() == false && computed.containsKey(samples.get(i)) ? 1 : 0;
            }
            listener.onResponse(new GroundTruthRunner.Result(stored, samples.size() + unreadable - stored));
        }, listener::onFailure));
    }
}
