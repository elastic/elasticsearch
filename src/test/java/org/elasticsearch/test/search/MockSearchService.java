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

package org.elasticsearch.test.search;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockSearchService extends SearchService {

    private static final Map<SearchContext, Throwable> ACTIVE_SEARCH_CONTEXTS = new ConcurrentHashMap<>();

    /** Throw an {@link AssertionError} if there are still in-flight contexts. */
    public static void assertNoInFLightContext() {
        final Map<SearchContext, Throwable> copy = new HashMap<>(ACTIVE_SEARCH_CONTEXTS);
        if (copy.isEmpty() == false) {
            throw new AssertionError("There are still " + copy.size() + " in-flight contexts", copy.values().iterator().next());
        }
    }

    @Inject
    public MockSearchService(Settings settings, ClusterService clusterService, IndicesService indicesService, IndicesWarmer indicesWarmer,
            ThreadPool threadPool, ScriptService scriptService, PageCacheRecycler pageCacheRecycler, BigArrays bigArrays,
            DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase, IndicesQueryCache indicesQueryCache) {
        super(settings, clusterService, indicesService, indicesWarmer, threadPool, scriptService, pageCacheRecycler, bigArrays, dfsPhase,
                queryPhase, fetchPhase, indicesQueryCache);
    }
 
    @Override
    protected void putContext(SearchContext context) {
        super.putContext(context);
        ACTIVE_SEARCH_CONTEXTS.put(context, new RuntimeException());
    }

    @Override
    protected SearchContext removeContext(long id) {
        final SearchContext removed = super.removeContext(id);
        if (removed != null) {
            ACTIVE_SEARCH_CONTEXTS.remove(removed);
        }
        return removed;
    }
}
