/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardSearchStatsTests extends ESTestCase {

    private static final long TEN_MILLIS = 10;

    public void testDfsPhase() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreDfsPhase(sc);
        listener.onDfsPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getDfsCurrent());
        assertEquals(1, stats.getTotal().getDfsCount());
        assertEquals(TEN_MILLIS, stats.getTotal().getDfsTimeInMillis());
    }

    public void testDfsPhase_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreDfsPhase(sc);
        listener.onFailedDfsPhase(sc);

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getDfsCurrent());
        assertEquals(0, stats.getTotal().getDfsCount());
        assertEquals(1, stats.getTotal().getDfsFailure());
    }

    public void testQueryPhase_SuggestOnly() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        SearchSourceBuilder ssb = new SearchSourceBuilder().suggest(new SuggestBuilder());
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);
        when(req.source()).thenReturn(ssb);

        listener.onPreQueryPhase(sc);
        listener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getSuggestCurrent());
        assertEquals(1, stats.getTotal().getSuggestCount());
        assertEquals(TEN_MILLIS, stats.getTotal().getSuggestTimeInMillis());
        assertEquals(0, stats.getTotal().getQueryCurrent());
        assertEquals(0, stats.getTotal().getQueryCount());
        assertEquals(0, stats.getTotal().getQueryTimeInMillis());
    }

    public void testQueryPhase() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreQueryPhase(sc);
        listener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getQueryCurrent());
        assertEquals(1, stats.getTotal().getQueryCount());
        assertEquals(TEN_MILLIS, stats.getTotal().getQueryTimeInMillis());
    }

    public void testQueryPhase_SuggestOnly_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        SearchSourceBuilder ssb = new SearchSourceBuilder().suggest(new SuggestBuilder());
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);
        when(req.source()).thenReturn(ssb);

        listener.onPreQueryPhase(sc);
        listener.onFailedQueryPhase(sc);

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getSuggestCurrent());
        assertEquals(0, stats.getTotal().getSuggestCount());
        assertEquals(0, stats.getTotal().getQueryCurrent());
        assertEquals(0, stats.getTotal().getQueryCount());
        assertEquals(0, stats.getTotal().getQueryFailure());
    }

    public void testQueryPhase_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreQueryPhase(sc);
        listener.onFailedQueryPhase(sc);

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getQueryCurrent());
        assertEquals(0, stats.getTotal().getQueryCount());
        assertEquals(1, stats.getTotal().getQueryFailure());
    }

    public void testFetchPhase() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreFetchPhase(sc);
        listener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getFetchCurrent());
        assertEquals(1, stats.getTotal().getFetchCount());
        assertEquals(TEN_MILLIS, stats.getTotal().getFetchTimeInMillis());
    }

    public void testFetchPhase_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreFetchPhase(sc);
        listener.onFailedFetchPhase(sc);

        SearchStats stats = listener.stats();
        assertEquals(0, stats.getTotal().getFetchCurrent());
        assertEquals(0, stats.getTotal().getFetchCount());
        assertEquals(1, stats.getTotal().getFetchFailure());
    }
}
