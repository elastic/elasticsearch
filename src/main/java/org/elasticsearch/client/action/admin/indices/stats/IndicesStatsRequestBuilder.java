/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.action.admin.indices.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.action.admin.indices.support.BaseIndicesRequestBuilder;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 *
 * <p>By default, the {@link #setDocs(boolean)}, {@link #setStore(boolean)}, {@link #setIndexing(boolean)}
 * are enabled. Other stats can be enabled as well.
 *
 * <p>All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequestBuilder extends BaseIndicesRequestBuilder<IndicesStatsRequest, IndicesStats> {

    public IndicesStatsRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new IndicesStatsRequest());
    }

    public IndicesStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Sets specific indices to return the stats for.
     */
    public IndicesStatsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
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

    @Override protected void doExecute(ActionListener<IndicesStats> listener) {
        client.stats(request, listener);
    }
}
