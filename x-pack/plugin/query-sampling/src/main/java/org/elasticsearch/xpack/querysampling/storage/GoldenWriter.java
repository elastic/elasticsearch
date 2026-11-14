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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Writes a new golden version in three steps: claim its number by creating the manifest (only one concurrent writer can win a
 * number), write the records, then complete the manifest. A writer that dies half way leaves an incomplete version that readers skip.
 */
public final class GoldenWriter {

    private static final Logger logger = LogManager.getLogger(GoldenWriter.class);

    // tries for a version number taken by another writer in the meantime
    private static final int ATTEMPTS = 5;

    public record Written(long version, int written, int failed) {}

    private final BiConsumer<SearchRequest, ActionListener<SearchResponse>> search;
    private final BiConsumer<IndexRequest, ActionListener<DocWriteResponse>> index;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk;
    private final BiConsumer<UpdateRequest, ActionListener<UpdateResponse>> update;
    private final LongSupplier clock;

    /** All requests run as the plugin, as nobody else may touch the golden dataset. */
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

    /** @param sourceVersion the version the queries come from, if any, kept in the manifest */
    public void write(List<StoredSample> queries, @Nullable Long sourceVersion, ActionListener<Written> listener) {
        // the latest version, completed or not
        GoldenReader.latest(
            search,
            QueryBuilders.termQuery("kind", GoldenIndex.VERSION),
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

    // creating the manifest fails if the number exists, then the next one is tried
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
                // readers skip a version that is not complete, so the next update starts again from the last good one
                String failure = response.buildFailureMessage();
                logger.warn(
                    "[{}] of [{}] golden queries could not be written, version [{}] is left incomplete: {}",
                    queries.size() - written,
                    queries.size(),
                    version,
                    failure
                );
                listener.onFailure(
                    new IllegalStateException(
                        "["
                            + (queries.size() - written)
                            + "] of ["
                            + queries.size()
                            + "] queries of version ["
                            + version
                            + "] could not be written"
                    )
                );
                return;
            }
            complete(version, written, 0, listener);
        }, listener::onFailure));
    }

    private void complete(long version, int written, int failed, ActionListener<Written> listener) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            UpdateRequest request = new UpdateRequest(GoldenIndex.NAME, GoldenRecord.manifestId(version)).retryOnConflict(3)
                .doc(GoldenRecord.completion(builder, written))
                // the next update finds this version by a search; the dataset is on one shard, so this also makes the records visible
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            update.accept(
                request,
                ActionListener.wrap(response -> listener.onResponse(new Written(version, written, failed)), listener::onFailure)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
