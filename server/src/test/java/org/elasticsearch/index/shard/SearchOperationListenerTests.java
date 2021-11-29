/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.ReaderContext;
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
import static org.mockito.Mockito.mock;

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
        AtomicInteger newScrollContext = new AtomicInteger();
        AtomicInteger freeScrollContext = new AtomicInteger();
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
            public void onNewReaderContext(ReaderContext readerContext) {
                assertNotNull(readerContext);
                newContext.incrementAndGet();
            }

            @Override
            public void onFreeReaderContext(ReaderContext readerContext) {
                assertNotNull(readerContext);
                freeContext.incrementAndGet();
            }

            @Override
            public void onNewScrollContext(ReaderContext readerContext) {
                assertNotNull(readerContext);
                newScrollContext.incrementAndGet();
            }

            @Override
            public void onFreeScrollContext(ReaderContext readerContext) {
                assertNotNull(readerContext);
                freeScrollContext.incrementAndGet();
            }

            @Override
            public void validateReaderContext(ReaderContext readerContext, TransportRequest request) {
                assertNotNull(readerContext);
                validateSearchContext.incrementAndGet();
            }
        };

        SearchOperationListener throwingListener = (SearchOperationListener) Proxy.newProxyInstance(
            SearchOperationListener.class.getClassLoader(),
            new Class<?>[] { SearchOperationListener.class },
            (a, b, c) -> { throw new RuntimeException(); }
        );
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
        SearchOperationListener.CompositeListener compositeListener = new SearchOperationListener.CompositeListener(
            indexingOperationListeners,
            logger
        );
        SearchContext ctx = new TestSearchContext((SearchExecutionContext) null);
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

        compositeListener.onNewReaderContext(mock(ReaderContext.class));
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

        compositeListener.onNewScrollContext(mock(ReaderContext.class));
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

        compositeListener.onFreeReaderContext(mock(ReaderContext.class));
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

        compositeListener.onFreeScrollContext(mock(ReaderContext.class));
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
            compositeListener.validateReaderContext(mock(ReaderContext.class), Empty.INSTANCE);
        } else {
            RuntimeException expected = expectThrows(
                RuntimeException.class,
                () -> compositeListener.validateReaderContext(mock(ReaderContext.class), Empty.INSTANCE)
            );
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
