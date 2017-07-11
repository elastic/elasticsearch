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

import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequest.Empty;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

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
        AtomicInteger validateSearchContext = new AtomicInteger();
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

            @Override
            public void validateSearchContext(SearchContext context, TransportRequest request) {
                assertNotNull(context);
                validateSearchContext.incrementAndGet();
            }
        };

        SearchOperationListener throwingListener = (SearchOperationListener) Proxy.newProxyInstance(
            SearchOperationListener.class.getClassLoader(),
            new Class[]{SearchOperationListener.class},
            (a,b,c) -> { throw new RuntimeException();});
        int throwingListeners = 0;
        final List<SearchOperationListener> indexingOperationListeners = new ArrayList<>(Arrays.asList(listener, listener));
        if (randomBoolean()) {
            indexingOperationListeners.add(throwingListener);
            throwingListeners++;
            if (randomBoolean()) {
                indexingOperationListeners.add(throwingListener);
                throwingListeners++;
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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

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
        assertEquals(0, validateSearchContext.get());

        if (throwingListeners == 0) {
            compositeListener.validateSearchContext(ctx, Empty.INSTANCE);
        } else {
            RuntimeException expected =
                expectThrows(RuntimeException.class, () -> compositeListener.validateSearchContext(ctx, Empty.INSTANCE));
            assertNull(expected.getMessage());
            assertEquals(throwingListeners - 1, expected.getSuppressed().length);
            if (throwingListeners > 1) {
                assertThat(expected.getSuppressed()[0], not(sameInstance(expected)));
            }
        }
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
        assertEquals(2, validateSearchContext.get());
    }
}
