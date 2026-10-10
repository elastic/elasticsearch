/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Promotes stored sampled queries that have their ground truth to a new version of the golden dataset.
 * <p>
 * A version is made in three steps. Its number is given out by creating its manifest, which only one promotion can do for a
 * number, so that two that run at the same time do not write to the same version. Then the records are written, and last
 * the manifest is completed with how many there are. A promotion that dies half way leaves a version that was never completed,
 * which no reader is to use.
 * <p>
 * A query is in a version once, even if more than one run of the sampler picked it: the one that was picked last is taken.
 * Arrivals that were kept as events are not promoted, a golden query is a query and not one of its searches.
 */
public final class GoldenPromoter {

    private static final Logger logger = LogManager.getLogger(GoldenPromoter.class);

    /**
     * How many times a number is tried for if it was taken by another promotion in the meantime.
     */
    private static final int ATTEMPTS = 5;

    /**
     * @param version  the version that was made, 0 if there was nothing to promote and none was made
     * @param promoted queries that are in the version
     * @param failed   queries that could not be written to it
     */
    public record Result(long version, int promoted, int failed) {}

    private final StoredSamples samples;
    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final BiConsumer<IndexRequest, ActionListener<DocWriteResponse>> index;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk;
    private final BiConsumer<UpdateRequest, ActionListener<UpdateResponse>> update;
    private final LongSupplier clock;

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
        this.search = search;
        this.index = index;
        this.bulk = bulk;
        this.update = update;
        this.clock = clock;
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
        latestVersion(
            ActionListener.wrap(
                latest -> claim(latest + 1, 1, ActionListener.wrap(version -> write(version, queries, listener), listener::onFailure)),
                listener::onFailure
            )
        );
    }

    /**
     * The number of the latest version, completed or not, 0 if there is none.
     */
    private void latestVersion(ActionListener<Long> listener) {
        SearchRequest request = new SearchRequest(GoldenIndex.NAME).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("kind", GoldenIndex.VERSION))
                .sort("dataset_version", SortOrder.DESC)
                .size(1)
        );
        search.accept(request, ActionListener.wrap(response -> {
            SearchHit[] hits = response.getHits().getHits();
            listener.onResponse(hits.length == 0 ? 0L : ((Number) hits[0].getSourceAsMap().get("dataset_version")).longValue());
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onResponse(0L);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Takes a number for a version by making its manifest, which fails if it exists. If the number was taken by another
     * promotion since it was looked up, the next one is tried.
     */
    private void claim(long version, int attempt, ActionListener<Long> listener) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            IndexRequest request = new IndexRequest(GoldenIndex.NAME).id(GoldenRecord.manifestId(version))
                .opType(DocWriteRequest.OpType.CREATE)
                .source(GoldenRecord.manifest(builder, version, clock.getAsLong(), 0, false));
            index.accept(request, ActionListener.wrap(response -> listener.onResponse(version), e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException && attempt < ATTEMPTS) {
                    claim(version + 1, attempt + 1, listener);
                } else {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void write(long version, List<StoredSample> queries, ActionListener<Result> listener) {
        BulkRequest request = new BulkRequest();
        long now = clock.getAsLong();
        for (StoredSample sample : queries) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                request.add(
                    new IndexRequest(GoldenIndex.NAME).id(GoldenRecord.recordId(version, sample.fingerprint()))
                        .opType(DocWriteRequest.OpType.CREATE)
                        .source(GoldenRecord.record(builder, version, sample, now))
                );
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }
        bulk.accept(request, ActionListener.wrap(response -> {
            int promoted = 0;
            for (BulkItemResponse item : response.getItems()) {
                promoted += item.isFailed() ? 0 : 1;
            }
            if (promoted < queries.size()) {
                logger.debug("[{}] queries could not be promoted: {}", queries.size() - promoted, response.buildFailureMessage());
            }
            complete(version, promoted, queries.size() - promoted, listener);
        }, listener::onFailure));
    }

    private void complete(long version, int promoted, int failed, ActionListener<Result> listener) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            UpdateRequest request = new UpdateRequest(GoldenIndex.NAME, GoldenRecord.manifestId(version)).retryOnConflict(3)
                .doc(GoldenRecord.completion(builder, promoted));
            update.accept(
                request,
                ActionListener.wrap(response -> listener.onResponse(new Result(version, promoted, failed)), listener::onFailure)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
