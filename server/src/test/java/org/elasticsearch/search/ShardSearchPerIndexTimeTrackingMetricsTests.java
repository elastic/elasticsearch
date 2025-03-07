/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.index.search.stats.ShardSearchPerIndexTimeTrackingMetrics;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardSearchPerIndexTimeTrackingMetricsTests extends ESTestCase {

    public void testSearchLoadForNotExistedIndex() {
        ShardSearchPerIndexTimeTrackingMetrics perIndexTimeTrackingMetrics = new ShardSearchPerIndexTimeTrackingMetrics(0.3);
        assertEquals(0L, perIndexTimeTrackingMetrics.getSearchLoadPerIndex("not_an_index"));
    }

    public void testSearchLoadForExistedIndex() {
        ShardSearchPerIndexTimeTrackingMetrics perIndexTimeTrackingMetrics = new ShardSearchPerIndexTimeTrackingMetrics(0.3);
        SearchContext sc = mock(SearchContext.class);
        IndexShard is = mock(IndexShard.class);
        ShardId sid = mock(ShardId.class);

        when(sc.indexShard()).thenReturn(is);
        when(is.isSystem()).thenReturn(false);
        when(is.shardId()).thenReturn(sid);
        when(sid.getIndexName()).thenReturn("test_index");

        perIndexTimeTrackingMetrics.onQueryPhase(sc, 1000L);
        perIndexTimeTrackingMetrics.onFailedQueryPhase(sc, 1000L);
        perIndexTimeTrackingMetrics.onFetchPhase(sc, 1000L);
        perIndexTimeTrackingMetrics.onFailedFetchPhase(sc, 1000L);

        assertEquals(4000L, perIndexTimeTrackingMetrics.getSearchLoadPerIndex("test_index"));
    }

    public void testLoadEMWAForNotExistedIndex() {
        ShardSearchPerIndexTimeTrackingMetrics perIndexTimeTrackingMetrics = new ShardSearchPerIndexTimeTrackingMetrics(0.3);
        assertEquals(0, perIndexTimeTrackingMetrics.getLoadEMWAPerIndex("not_an_index"), 0.1);
    }

    public void testLoadEMWAForExistedIndex() {
        ShardSearchPerIndexTimeTrackingMetrics perIndexTimeTrackingMetrics = new ShardSearchPerIndexTimeTrackingMetrics(0.3);
        SearchContext sc = mock(SearchContext.class);
        IndexShard is = mock(IndexShard.class);
        ShardId sid = mock(ShardId.class);

        when(sc.indexShard()).thenReturn(is);
        when(is.isSystem()).thenReturn(false);
        when(is.shardId()).thenReturn(sid);
        when(sid.getIndexName()).thenReturn("test_index");

        perIndexTimeTrackingMetrics.onQueryPhase(sc, 1000L);
        assertTrue(perIndexTimeTrackingMetrics.getLoadEMWAPerIndex("test_index") > 0);
    }

    public void testStopTrackingAnIndex() {
        ShardSearchPerIndexTimeTrackingMetrics perIndexTimeTrackingMetrics = new ShardSearchPerIndexTimeTrackingMetrics(0.3);
        SearchContext sc = mock(SearchContext.class);
        IndexShard is = mock(IndexShard.class);
        ShardId sid = mock(ShardId.class);

        when(sc.indexShard()).thenReturn(is);
        when(is.isSystem()).thenReturn(false);
        when(is.shardId()).thenReturn(sid);
        when(sid.getIndexName()).thenReturn("test_index");

        perIndexTimeTrackingMetrics.stopTrackingIndex("test_index");
        assertEquals(0L, perIndexTimeTrackingMetrics.getSearchLoadPerIndex("test_index"));
    }

}
