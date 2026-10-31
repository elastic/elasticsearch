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
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Computes and stores the ground truth of sampled queries in {@link QuerySamplingIndex} that lack it; any node can do it.
 * The sample index is accessed as the plugin, while exact searches run as the caller so they only see what that person may see.
 */
public final class StoredGroundTruth {

    private static final Logger logger = LogManager.getLogger(StoredGroundTruth.class);

    private final StoredSamples samples;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> sampleBulk;
    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch;
    private final LongSupplier clock;
    private final Executor executor;

    public StoredGroundTruth(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> sampleSearch,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> sampleBulk,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch,
        NamedXContentRegistry registry,
        LongSupplier clock,
        Executor executor
    ) {
        this.samples = new StoredSamples(sampleSearch, registry);
        this.sampleBulk = sampleBulk;
        this.exactSearch = exactSearch;
        this.clock = clock;
        this.executor = executor;
    }

    /**
     * Computes up to {@code max} stored queries, longest waiting first. A failed one moves to the back of the line so it
     * does not starve the others.
     */
    public void compute(int max, ActionListener<GroundTruthRunner.Result> listener) {
        compute(max, null, listener);
    }

    /**
     * @param filter on the fields of the stored documents, or {@code null} for all that are pending
     */
    public void compute(int max, @Nullable QueryBuilder filter, ActionListener<GroundTruthRunner.Result> listener) {
        QueryBuilder pending = QueryBuilders.termQuery("has_ground_truth", false);
        samples.read(
            filter == null ? pending : QueryBuilders.boolQuery().filter(pending).filter(filter),
            "updated_at",
            SortOrder.ASC,
            max,
            ActionListener.wrap(read -> compute(read.samples(), read.unreadableIds(), listener), listener::onFailure)
        );
    }

    private void compute(List<StoredSample> samples, List<String> unreadable, ActionListener<GroundTruthRunner.Result> listener) {
        Map<StoredSample, GroundTruth> computed = new IdentityHashMap<>();
        new GroundTruthRunner(exactSearch, executor).run(
            samples,
            sample -> sample.search().query(),
            computed::put,
            ActionListener.wrap(result -> store(samples, computed, unreadable, listener), listener::onFailure)
        );
    }

    private void store(
        List<StoredSample> samples,
        Map<StoredSample, GroundTruth> computed,
        List<String> unreadable,
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
        // a document that cannot be read is looked at too: left as it is, it would stay first in line and starve the others
        for (String id : unreadable) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                request.add(new UpdateRequest(QuerySamplingIndex.NAME, id).retryOnConflict(3).doc(SampleRecord.touch(builder, now)));
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }
        if (request.numberOfActions() == 0) {
            listener.onResponse(new GroundTruthRunner.Result(0, 0));
            return;
        }
        sampleBulk.accept(request, ActionListener.wrap(response -> {
            // the items are in the order of the request, which is the order of the samples
            int stored = 0;
            BulkItemResponse[] items = response.getItems();
            assert items.length == request.numberOfActions() : "a bulk response has an item for each action";
            for (int i = 0; i < samples.size(); i++) {
                stored += items[i].isFailed() == false && computed.containsKey(samples.get(i)) ? 1 : 0;
            }
            listener.onResponse(new GroundTruthRunner.Result(stored, samples.size() + unreadable.size() - stored));
        }, listener::onFailure));
    }
}
