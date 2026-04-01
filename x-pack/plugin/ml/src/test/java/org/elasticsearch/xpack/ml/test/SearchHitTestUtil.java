/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Helpers for pooled {@link SearchHit} lifecycle in ML unit tests.
 * <p>
 * Pooled hits are {@link org.elasticsearch.transport.LeakTracker}-tracked: each {@code new SearchHit(...)} starts with
 * ref count 1. {@link SearchHits#decRef()} runs {@link SearchHit#decRef()} on each hit when the container is released.
 * Mock {@link SearchResponse} must stub {@link SearchResponse#decRef()} to call {@link SearchHits#decRef()} on the
 * backing hits, matching production behavior. Batches from {@code DataFrameDataExtractor#next()} are the top-level
 * {@link SearchHits}; the caller must {@link SearchHits#decRef()} that instance once per batch when done.
 */
public final class SearchHitTestUtil {

    private SearchHitTestUtil() {}

    /**
     * Same as {@link ActionListener#respondAndRelease(ActionListener, org.elasticsearch.core.RefCounted)} for
     * {@link SearchResponse}: invokes the listener then {@link SearchResponse#decRef()} in a {@code finally} block,
     * matching coordinating-node transport behavior.
     */
    public static void respondAndReleaseSearchResponse(ActionListener<SearchResponse> listener, SearchResponse response) {
        ActionListener.respondAndRelease(listener, response);
    }

    /**
     * When tests time out or stop early, some mock {@link SearchResponse} instances may never be passed through
     * {@link #respondAndReleaseSearchResponse}; release any remaining pooled {@link SearchHits} refs.
     */
    public static void releaseUnusedPooledSearchResponses(Iterable<SearchResponse> responses) {
        for (SearchResponse r : responses) {
            SearchHits h = r.getHits();
            if (h.isPooled()) {
                while (h.hasReferences()) {
                    h.decRef();
                }
            }
        }
    }

    /**
     * Spy a real {@link SearchResponse} built via {@link org.elasticsearch.search.SearchResponseUtils} so
     * {@link SearchResponse#decRef()} fully drains pooled {@link SearchHits}: the real constructor calls
     * {@code hits.incRef()}, and one {@code hits.decRef()} from the real {@code decRef} can leave the container
     * referenced; an extra {@code hits.decRef()} matches what {@link org.elasticsearch.test.ESTestCase#checkStaticState} expects.
     */
    public static SearchResponse spySearchResponseDrainingPooledHits(SearchResponse built, SearchHits pooledHits) {
        SearchResponse spied = spy(built);
        doAnswer(invocation -> {
            boolean released = (boolean) invocation.callRealMethod();
            if (pooledHits.hasReferences()) {
                pooledHits.decRef();
            }
            return released;
        }).when(spied).decRef();
        return spied;
    }

    /**
     * DecRefs every non-null hit in an array. Not used for {@code DataFrameDataExtractor#next()} batches
     * (those use {@link SearchHits#decRef()} on the container).
     */
    public static void decRefHits(SearchHit[] hits) {
        if (hits == null) {
            return;
        }
        for (SearchHit h : hits) {
            if (h != null) {
                h.decRef();
            }
        }
    }

    /**
     * Wire a Mockito {@link SearchResponse} so {@link SearchResponse#decRef()} releases the given {@link SearchHits},
     * like a real response. Use with pooled hits built via {@link SearchHitBuilder}.
     */
    public static void stubSearchResponseDecRefsHits(SearchResponse searchResponse, SearchHits searchHits) {
        doAnswer(inv -> {
            searchHits.decRef();
            return true;
        }).when(searchResponse).decRef();
    }
}
