/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 * <p>
 * By default, the {@link #setDocs(boolean)}, {@link #setStore(boolean)}, {@link #setIndexing(boolean)}
 * are enabled. Other stats can be enabled as well.
 * <p>
 * All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequestBuilder extends BroadcastOperationRequestBuilder<
    IndicesStatsRequest,
    IndicesStatsResponse,
    IndicesStatsRequestBuilder> {

    public IndicesStatsRequestBuilder(ElasticsearchClient client, IndicesStatsAction action) {
        super(client, action, new IndicesStatsRequest());
    }

    /**
     * Sets all flags to return all stats.
     */
    public IndicesStatsRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats.
     */
    public IndicesStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    public IndicesStatsRequestBuilder setGroups(String... groups) {
        request.groups(groups);
        return this;
    }

    public IndicesStatsRequestBuilder setDocs(boolean docs) {
        request.docs(docs);
        return this;
    }

    public IndicesStatsRequestBuilder setStore(boolean store) {
        request.store(store);
        return this;
    }

    public IndicesStatsRequestBuilder setIndexing(boolean indexing) {
        request.indexing(indexing);
        return this;
    }

    public IndicesStatsRequestBuilder setGet(boolean get) {
        request.get(get);
        return this;
    }

    public IndicesStatsRequestBuilder setSearch(boolean search) {
        request.search(search);
        return this;
    }

    public IndicesStatsRequestBuilder setMerge(boolean merge) {
        request.merge(merge);
        return this;
    }

    public IndicesStatsRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public IndicesStatsRequestBuilder setFlush(boolean flush) {
        request.flush(flush);
        return this;
    }

    public IndicesStatsRequestBuilder setWarmer(boolean warmer) {
        request.warmer(warmer);
        return this;
    }

    public IndicesStatsRequestBuilder setQueryCache(boolean queryCache) {
        request.queryCache(queryCache);
        return this;
    }

    public IndicesStatsRequestBuilder setFieldData(boolean fieldData) {
        request.fieldData(fieldData);
        return this;
    }

    public IndicesStatsRequestBuilder setFieldDataFields(String... fields) {
        request.fieldDataFields(fields);
        return this;
    }

    public IndicesStatsRequestBuilder setSegments(boolean segments) {
        request.segments(segments);
        return this;
    }

    public IndicesStatsRequestBuilder setCompletion(boolean completion) {
        request.completion(completion);
        return this;
    }

    public IndicesStatsRequestBuilder setCompletionFields(String... fields) {
        request.completionFields(fields);
        return this;
    }

    public IndicesStatsRequestBuilder setTranslog(boolean translog) {
        request.translog(translog);
        return this;
    }

    public IndicesStatsRequestBuilder setRequestCache(boolean requestCache) {
        request.requestCache(requestCache);
        return this;
    }

    public IndicesStatsRequestBuilder setRecovery(boolean recovery) {
        request.recovery(recovery);
        return this;
    }

    public IndicesStatsRequestBuilder setBulk(boolean bulk) {
        request.bulk(bulk);
        return this;
    }

    public IndicesStatsRequestBuilder setIncludeSegmentFileSizes(boolean includeSegmentFileSizes) {
        request.includeSegmentFileSizes(includeSegmentFileSizes);
        return this;
    }

    public IndicesStatsRequestBuilder setIncludeUnloadedSegments(boolean includeUnloadedSegments) {
        request.includeUnloadedSegments(includeUnloadedSegments);
        return this;
    }
}
