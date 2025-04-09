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
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Objects;
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

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getDfsCurrent());
        assertEquals(1, stats.getDfsCount());
        assertEquals(TEN_MILLIS, stats.getDfsTimeInMillis());
    }

    public void testDfsPhase_withGroups() {
        String[] groups = new String[] { "group1" };

        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(Arrays.asList(groups));

        listener.onPreDfsPhase(sc);
        listener.onDfsPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats searchStats = listener.stats(groups);
        SearchStats.Stats stats = searchStats.getTotal();
        assertEquals(0, stats.getDfsCurrent());
        assertEquals(1, stats.getDfsCount());
        assertEquals(TEN_MILLIS, stats.getDfsTimeInMillis());

        stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
        assertEquals(0, stats.getDfsCurrent());
        assertEquals(1, stats.getDfsCount());
        assertEquals(TEN_MILLIS, stats.getDfsTimeInMillis());
    }

    public void testDfsPhase_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreDfsPhase(sc);
        listener.onFailedDfsPhase(sc);

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getDfsCurrent());
        assertEquals(0, stats.getDfsCount());
        assertEquals(1, stats.getDfsFailure());
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

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getSuggestCurrent());
        assertEquals(1, stats.getSuggestCount());
        assertEquals(TEN_MILLIS, stats.getSuggestTimeInMillis());
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(0, stats.getQueryCount());
        assertEquals(0, stats.getQueryTimeInMillis());
    }

    public void testQueryPhase() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreQueryPhase(sc);
        listener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(1, stats.getQueryCount());
        assertEquals(TEN_MILLIS, stats.getQueryTimeInMillis());
    }

    public void testQueryPhase_withGroups() {
        String[] groups = new String[] { "group1" };

        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(Arrays.asList(groups));

        listener.onPreQueryPhase(sc);
        listener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats searchStats = listener.stats("_all");
        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(1, stats.getQueryCount());
        assertEquals(TEN_MILLIS, stats.getQueryTimeInMillis());

        stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(1, stats.getQueryCount());
        assertEquals(TEN_MILLIS, stats.getQueryTimeInMillis());
    }

    public void testQueryPhase_withGroups_SuggestOnly() {
        String[] groups = new String[] { "group1" };

        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        SearchSourceBuilder ssb = new SearchSourceBuilder().suggest(new SuggestBuilder());
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);
        when(req.source()).thenReturn(ssb);
        when(sc.groupStats()).thenReturn(Arrays.asList(groups));

        listener.onPreQueryPhase(sc);
        listener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats searchStats = listener.stats("_all");
        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getSuggestCurrent());
        assertEquals(1, stats.getSuggestCount());
        assertEquals(TEN_MILLIS, stats.getSuggestTimeInMillis());
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(0, stats.getQueryCount());
        assertEquals(0, stats.getQueryTimeInMillis());

        stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
        assertEquals(0, stats.getSuggestCurrent());
        assertEquals(1, stats.getSuggestCount());
        assertEquals(TEN_MILLIS, stats.getSuggestTimeInMillis());
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(0, stats.getQueryCount());
        assertEquals(0, stats.getQueryTimeInMillis());
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

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getSuggestCurrent());
        assertEquals(0, stats.getSuggestCount());
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(0, stats.getQueryCount());
        assertEquals(0, stats.getQueryFailure());
    }

    public void testQueryPhase_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreQueryPhase(sc);
        listener.onFailedQueryPhase(sc);

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getQueryCurrent());
        assertEquals(0, stats.getQueryCount());
        assertEquals(1, stats.getQueryFailure());
    }

    public void testFetchPhase() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        ShardSearchRequest req = mock(ShardSearchRequest.class);
        when(sc.request()).thenReturn(req);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreFetchPhase(sc);
        listener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getFetchCurrent());
        assertEquals(1, stats.getFetchCount());
        assertEquals(TEN_MILLIS, stats.getFetchTimeInMillis());
    }

    public void testFetchPhase_withGroups() {
        String[] groups = new String[] { "group1" };

        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(Arrays.asList(groups));

        listener.onPreFetchPhase(sc);
        listener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats searchStats = listener.stats("_all");
        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getFetchCurrent());
        assertEquals(1, stats.getFetchCount());
        assertEquals(TEN_MILLIS, stats.getFetchTimeInMillis());

        stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
        assertEquals(0, stats.getFetchCurrent());
        assertEquals(1, stats.getFetchCount());
        assertEquals(TEN_MILLIS, stats.getFetchTimeInMillis());
    }

    public void testFetchPhase_Failure() {
        ShardSearchStats listener = new ShardSearchStats();
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onPreFetchPhase(sc);
        listener.onFailedFetchPhase(sc);

        SearchStats.Stats stats = listener.stats().getTotal();
        assertEquals(0, stats.getFetchCurrent());
        assertEquals(0, stats.getFetchCount());
        assertEquals(1, stats.getFetchFailure());
    }

    public void testReaderContext() {
        ShardSearchStats listener = new ShardSearchStats();
        ReaderContext rc = mock(ReaderContext.class);
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onNewReaderContext(rc);
        SearchStats stats = listener.stats();
        assertEquals(1, stats.getOpenContexts());

        listener.onFreeReaderContext(rc);
        stats = listener.stats();
        assertEquals(0, stats.getOpenContexts());
    }

    public void testScrollContext() {
        ShardSearchStats listener = new ShardSearchStats();
        ReaderContext rc = mock(ReaderContext.class);
        SearchContext sc = mock(SearchContext.class);
        when(sc.groupStats()).thenReturn(null);

        listener.onNewScrollContext(rc);
        SearchStats stats = listener.stats();
        assertEquals(1, stats.getTotal().getScrollCurrent());

        listener.onFreeScrollContext(rc);
        stats = listener.stats();
        assertEquals(0, stats.getTotal().getScrollCurrent());
        assertEquals(1, stats.getTotal().getScrollCount());
        assertTrue(stats.getTotal().getScrollTimeInMillis() > 0);
    }
}
