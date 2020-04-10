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

package org.elasticsearch.search;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockSearchService extends SearchService {
    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockSearchService}.
     */
    public static class TestPlugin extends Plugin {}

    private static final Map<SearchContext, Throwable> ACTIVE_SEARCH_CONTEXTS = new ConcurrentHashMap<>();

    /** Throw an {@link AssertionError} if there are still in-flight contexts. */
    public static void assertNoInFlightContext() {
        final Map<SearchContext, Throwable> copy = new HashMap<>(ACTIVE_SEARCH_CONTEXTS);
        if (copy.isEmpty() == false) {
            throw new AssertionError(
                    "There are still [" + copy.size()
                            + "] in-flight contexts. The first one's creation site is listed as the cause of this exception.",
                    copy.values().iterator().next());
        }
    }

    /**
     * Add an active search context to the list of tracked contexts. Package private for testing.
     */
    static void addActiveContext(SearchContext context) {
        ACTIVE_SEARCH_CONTEXTS.put(context, new RuntimeException(context.toString()));
    }

    /**
     * Clear an active search context from the list of tracked contexts. Package private for testing.
     */
    static void removeActiveContext(SearchContext context) {
        ACTIVE_SEARCH_CONTEXTS.remove(context);
    }

    public MockSearchService(ClusterService clusterService,
            IndicesService indicesService, ThreadPool threadPool, ScriptService scriptService,
            BigArrays bigArrays, FetchPhase fetchPhase, CircuitBreakerService circuitBreakerService) {
        super(clusterService, indicesService, threadPool, scriptService, bigArrays, fetchPhase, null, circuitBreakerService);
    }

    @Override
    protected void putContext(SearchContext context) {
        addActiveContext(context);
        super.putContext(context);
    }

    @Override
    protected SearchContext removeContext(long id) {
        final SearchContext removed = super.removeContext(id);
        if (removed != null) {
            removeActiveContext(removed);
        }
        return removed;
    }
}
