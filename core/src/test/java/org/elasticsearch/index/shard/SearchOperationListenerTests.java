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
package org.elasticsearch.index.shard;

import org.apache.lucene.index.Term;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SearchOperationListenerTests extends ESTestCase {

    // this test also tests if calls are correct if one or more listeners throw exceptions
    public void testListenersAreExecuted() {
        AtomicInteger preQuery = new AtomicInteger();
        AtomicInteger failedQuery = new AtomicInteger();
        AtomicInteger onQuery = new AtomicInteger();
        AtomicInteger onFetch = new AtomicInteger();
        AtomicInteger preFetch = new AtomicInteger();
        AtomicInteger failedFetch = new AtomicInteger();
        AtomicInteger newContext = new AtomicInteger();
        AtomicInteger freeContext = new AtomicInteger();
        AtomicInteger newScrollContext =  new AtomicInteger();
        AtomicInteger freeScrollContext =  new AtomicInteger();
        AtomicInteger timeInNanos = new AtomicInteger(randomIntBetween(0, 10));
        SearchOperationListener listener = new SearchOperationListener() {
            @Override
            public void onPreQueryPhase(SearchContext searchContext) {
                assertNotNull(searchContext);
                preQuery.incrementAndGet();
            }

            @Override
            public void onFailedQueryPhase(SearchContext searchContext) {
                assertNotNull(searchContext);
                failedQuery.incrementAndGet();
            }

            @Override
            public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                assertEquals(timeInNanos.get(), tookInNanos);
                assertNotNull(searchContext);
                onQuery.incrementAndGet();
            }

            @Override
            public void onPreFetchPhase(SearchContext searchContext) {
                assertNotNull(searchContext);
                preFetch.incrementAndGet();

            }

            @Override
            public void onFailedFetchPhase(SearchContext searchContext) {
                assertNotNull(searchContext);
                failedFetch.incrementAndGet();
            }

            @Override
            public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
                assertEquals(timeInNanos.get(), tookInNanos);
                onFetch.incrementAndGet();
            }

            @Override
            public void onNewContext(SearchContext context) {
                assertNotNull(context);
                newContext.incrementAndGet();
            }

            @Override
            public void onFreeContext(SearchContext context) {
                assertNotNull(context);
                freeContext.incrementAndGet();
            }

            @Override
            public void onNewScrollContext(SearchContext context) {
                assertNotNull(context);
                newScrollContext.incrementAndGet();
            }

            @Override
            public void onFreeScrollContext(SearchContext context) {
                assertNotNull(context);
                freeScrollContext.incrementAndGet();
            }
        };

        SearchOperationListener throwingListener = (SearchOperationListener) Proxy.newProxyInstance(
            SearchOperationListener.class.getClassLoader(),
            new Class[]{SearchOperationListener.class},
            (a,b,c) -> { throw new RuntimeException();});
        final List<SearchOperationListener> indexingOperationListeners = new ArrayList<>(Arrays.asList(listener, listener));
        if (randomBoolean()) {
            indexingOperationListeners.add(throwingListener);
            if (randomBoolean()) {
                indexingOperationListeners.add(throwingListener);
            }
        }
        Collections.shuffle(indexingOperationListeners, random());
        SearchOperationListener.CompositeListener compositeListener =
            new SearchOperationListener.CompositeListener(indexingOperationListeners, logger);
        SearchContext ctx = new TestSearchContext(null);
        compositeListener.onQueryPhase(ctx, timeInNanos.get());
        assertEquals(0, preFetch.get());
        assertEquals(0, preQuery.get());
        assertEquals(0, failedFetch.get());
        assertEquals(0, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(0, onFetch.get());
        assertEquals(0, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onFetchPhase(ctx, timeInNanos.get());
        assertEquals(0, preFetch.get());
        assertEquals(0, preQuery.get());
        assertEquals(0, failedFetch.get());
        assertEquals(0, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(0, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onPreQueryPhase(ctx);
        assertEquals(0, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(0, failedFetch.get());
        assertEquals(0, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(0, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onPreFetchPhase(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(0, failedFetch.get());
        assertEquals(0, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(0, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onFailedFetchPhase(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(2, failedFetch.get());
        assertEquals(0, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(0, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onFailedQueryPhase(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(2, failedFetch.get());
        assertEquals(2, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(0, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onNewContext(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(2, failedFetch.get());
        assertEquals(2, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(2, newContext.get());
        assertEquals(0, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onNewScrollContext(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(2, failedFetch.get());
        assertEquals(2, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(2, newContext.get());
        assertEquals(2, newScrollContext.get());
        assertEquals(0, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onFreeContext(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(2, failedFetch.get());
        assertEquals(2, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(2, newContext.get());
        assertEquals(2, newScrollContext.get());
        assertEquals(2, freeContext.get());
        assertEquals(0, freeScrollContext.get());

        compositeListener.onFreeScrollContext(ctx);
        assertEquals(2, preFetch.get());
        assertEquals(2, preQuery.get());
        assertEquals(2, failedFetch.get());
        assertEquals(2, failedQuery.get());
        assertEquals(2, onQuery.get());
        assertEquals(2, onFetch.get());
        assertEquals(2, newContext.get());
        assertEquals(2, newScrollContext.get());
        assertEquals(2, freeContext.get());
        assertEquals(2, freeScrollContext.get());
    }
}
