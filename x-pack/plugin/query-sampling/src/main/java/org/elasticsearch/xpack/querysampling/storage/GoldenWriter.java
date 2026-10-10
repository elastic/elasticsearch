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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Writes a new version of the golden dataset, however its queries came to be.
 * <p>
 * A version is made in three steps. Its number is given out by creating its manifest, which only one writer can do for a
 * number, so that two that run at the same time do not write to the same version. Then the records are written, and last
 * the manifest is completed with how many there are. A writer that dies half way leaves a version that was never completed,
 * which no reader is to use.
 */
public final class GoldenWriter {

    private static final Logger logger = LogManager.getLogger(GoldenWriter.class);

    /**
     * How many times a number is tried for if it was taken by another writer in the meantime.
     */
    private static final int ATTEMPTS = 5;

    /**
     * @param version the version that was made
     * @param written queries that are in the version
     * @param failed  queries that could not be written to it
     */
    public record Written(long version, int written, int failed) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final BiConsumer<IndexRequest, ActionListener<DocWriteResponse>> index;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk;
    private final BiConsumer<UpdateRequest, ActionListener<UpdateResponse>> update;
    private final LongSupplier clock;

    /**
     * All of them are done as the plugin, as nobody else may touch the golden dataset.
     *
     * @param search searches the golden dataset
     * @param clock  milliseconds since the epoch
     */
    public GoldenWriter(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search,
        BiConsumer<IndexRequest, ActionListener<DocWriteResponse>> index,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk,
        BiConsumer<UpdateRequest, ActionListener<UpdateResponse>> update,
        LongSupplier clock
    ) {
        this.search = search;
        this.index = index;
        this.bulk = bulk;
        this.update = update;
        this.clock = clock;
    }

    /**
     * Makes a new version of the dataset that has these queries, each with the ground truth it has.
     *
     * @param sourceVersion the version that the queries come from, if they come from one, which is kept in the manifest
     */
    public void write(List<StoredSample> queries, @Nullable Long sourceVersion, ActionListener<Written> listener) {
        latestVersion(
            ActionListener.wrap(
                latest -> claim(
                    latest + 1,
                    1,
                    sourceVersion,
                    ActionListener.wrap(version -> writeRecords(version, queries, listener), listener::onFailure)
                ),
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
     * writer since it was looked up, the next one is tried.
     */
    private void claim(long version, int attempt, @Nullable Long sourceVersion, ActionListener<Long> listener) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            IndexRequest request = new IndexRequest(GoldenIndex.NAME).id(GoldenRecord.manifestId(version))
                .opType(DocWriteRequest.OpType.CREATE)
                .source(GoldenRecord.manifest(builder, version, clock.getAsLong(), 0, false, sourceVersion));
            index.accept(request, ActionListener.wrap(response -> listener.onResponse(version), e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException && attempt < ATTEMPTS) {
                    claim(version + 1, attempt + 1, sourceVersion, listener);
                } else {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void writeRecords(long version, List<StoredSample> queries, ActionListener<Written> listener) {
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
            int written = 0;
            for (BulkItemResponse item : response.getItems()) {
                written += item.isFailed() ? 0 : 1;
            }
            if (written < queries.size()) {
                logger.debug("[{}] queries could not be written: {}", queries.size() - written, response.buildFailureMessage());
            }
            complete(version, written, queries.size() - written, listener);
        }, listener::onFailure));
    }

    private void complete(long version, int written, int failed, ActionListener<Written> listener) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            UpdateRequest request = new UpdateRequest(GoldenIndex.NAME, GoldenRecord.manifestId(version)).retryOnConflict(3)
                .doc(GoldenRecord.completion(builder, written));
            update.accept(
                request,
                ActionListener.wrap(response -> listener.onResponse(new Written(version, written, failed)), listener::onFailure)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
