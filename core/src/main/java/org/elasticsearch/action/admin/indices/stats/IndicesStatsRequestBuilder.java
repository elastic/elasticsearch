/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 * <p>
 * By default, the {@link #setDocs(boolean)}, {@link #setStore(boolean)}, {@link #setIndexing(boolean)}
 * are enabled. Other stats can be enabled as well.
 * <p>
 * All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequestBuilder extends BroadcastOperationRequestBuilder<IndicesStatsRequest, IndicesStatsResponse, IndicesStatsRequestBuilder> {

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

    /**
     * Document types to return stats for. Mainly affects {@link #setIndexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public IndicesStatsRequestBuilder setTypes(String... types) {
        request.types(types);
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

    public IndicesStatsRequestBuilder setIncludeSegmentFileSizes(boolean includeSegmentFileSizes) {
        request.includeSegmentFileSizes(includeSegmentFileSizes);
        return this;
    }
}
