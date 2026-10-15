/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.List;

/** Shard accounting across native paging, failure callbacks and independently completed ordinary-compute contributions. */
public class SequenceExecutionInfoTests extends ESTestCase {
    public void testPagesRetainShardsOmittedBySnapshotCanMatch() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(new OpenPointInTimeResponse(new BytesArray("snapshot"), 5, 5, 0, 2, SearchResponse.Clusters.EMPTY));
        for (int page = 0; page < 3; page++) {
            SearchResponse response = searchResponse(3, 3, 0, false);
            try {
                tracker.onSearch(response);
            } finally {
                response.decRef();
            }
            assertCounts(info, 5, 5, 2, 0);
        }
        var failure = new ShardSearchFailure(new IllegalStateException("unavailable retained shard"));
        SearchResponse response = searchResponse(3, 2, 1, false, failure);
        try {
            tracker.onSearch(response);
        } finally {
            response.decRef();
        }
        assertCounts(info, 5, 4, 3, 1);
        assertEquals(EsqlExecutionInfo.Cluster.Status.FAILED, cluster(info).getStatus());
    }

    public void testSearchPhaseFailureCannotFailShardsOmittedBySnapshotCanMatch() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(new OpenPointInTimeResponse(new BytesArray("snapshot"), 3, 3, 0, 2, SearchResponse.Clusters.EMPTY));
        var failure = new ShardSearchFailure(new IllegalStateException("unavailable retained shard"));
        tracker.onFailure(
            new SearchPhaseExecutionException("query", "failed source search", new ShardSearchFailure[] { failure, failure })
        );
        assertCounts(info, 3, 2, 2, 1);
        assertEquals(EsqlExecutionInfo.Cluster.Status.FAILED, cluster(info).getStatus());
    }

    public void testRepeatedPageAndPayloadSearchesCountSnapshotShardsOnce() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(openResponse(4));
        for (int page = 0; page < 10; page++) {
            SearchResponse response = searchResponse(4, 4, 0, false);
            try {
                tracker.onSearch(response);
            } finally {
                response.decRef();
            }
        }
        assertCounts(info, 4, 4, 0, 0);
        assertEquals(EsqlExecutionInfo.Cluster.Status.RUNNING, cluster(info).getStatus());
        assertTrue(cluster(info).getFailures().isEmpty());
    }

    public void testNativeAndDataNodeCompletionOrderPreservesCounts() {
        for (boolean dataNodeFirst : List.of(false, true)) {
            EsqlExecutionInfo info = executionInfo();
            if (dataNodeFirst) {
                info.setDataNodeShardCounts(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 3, 3, 0, 0, List.of());
            }
            var first = new SequenceExecutionInfo(info);
            var second = new SequenceExecutionInfo(info);
            first.onOpen(openResponse(4));
            second.onOpen(openResponse(2));
            if (dataNodeFirst == false) {
                info.setDataNodeShardCounts(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 3, 3, 0, 0, List.of());
            }
            SearchResponse response = searchResponse(4, 4, 0, false);
            try {
                first.onSearch(response);
            } finally {
                response.decRef();
            }
            assertCounts(info, 9, 9, 0, 0);
        }
    }

    public void testMergeBranchesAccumulateWithoutDoubleCountingNativeSource() {
        for (boolean nativeFirst : List.of(false, true)) {
            EsqlExecutionInfo info = executionInfo();
            var tracker = new SequenceExecutionInfo(info);
            if (nativeFirst) {
                tracker.onOpen(openResponse(4));
            }
            info.setDataNodeShardCounts(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 3, 3, 0, 0, List.of(), true);
            if (nativeFirst == false) {
                tracker.onOpen(openResponse(4));
            }
            info.setDataNodeShardCounts(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 2, 2, 0, 0, List.of(), true);
            assertCounts(info, 9, 9, 0, 0);
            var failure = new ShardSearchFailure(new IllegalStateException("unavailable retained shard"));
            SearchResponse response = searchResponse(4, 3, 1, false, failure);
            try {
                tracker.onSearch(response);
            } finally {
                response.decRef();
            }
            assertCounts(info, 9, 8, 1, 1);
        }
    }

    public void testIncompleteSearchReplacesOpenedSuccessAndReportsFailureOnce() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(openResponse(4));
        var failure = new ShardSearchFailure(new IllegalStateException("unavailable shard"));
        SearchResponse response = searchResponse(4, 3, 1, false, failure);
        try {
            tracker.onSearch(response);
            tracker.onFailure(new IllegalStateException("complete search required"));
        } finally {
            response.decRef();
        }
        assertCounts(info, 4, 3, 1, 1);
        assertEquals(EsqlExecutionInfo.Cluster.Status.FAILED, cluster(info).getStatus());
        assertEquals(List.of(failure), cluster(info).getFailures());
        assertTrue(info.isPartial());
    }

    public void testTimedOutSearchPreservesRespondingShardCounts() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(openResponse(4));
        SearchResponse response = searchResponse(4, 4, 0, true);
        try {
            tracker.onSearch(response);
        } finally {
            response.decRef();
        }
        assertCounts(info, 4, 4, 0, 0);
        assertEquals(EsqlExecutionInfo.Cluster.Status.FAILED, cluster(info).getStatus());
        assertEquals(1, cluster(info).getFailures().size());
        assertTrue(cluster(info).getFailures().getFirst().getCause().getMessage().contains("timed out"));
        assertTrue(info.isPartial());
    }

    public void testSearchPhaseFailureAfterOpenUpdatesKnownFailedShards() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(openResponse(4));
        var failure = new ShardSearchFailure(new IllegalStateException("failed query phase"));
        tracker.onFailure(new SearchPhaseExecutionException("query", "failed source search", new ShardSearchFailure[] { failure }));
        info.setDataNodeShardCounts(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, 2, 2, 0, 0, List.of());
        assertCounts(info, 6, 5, 0, 1);
        assertEquals(EsqlExecutionInfo.Cluster.Status.FAILED, cluster(info).getStatus());
        assertEquals(List.of(failure), cluster(info).getFailures());
    }

    public void testCoordinatorFailureDoesNotInventFailedShards() {
        EsqlExecutionInfo info = executionInfo();
        var tracker = new SequenceExecutionInfo(info);
        tracker.onOpen(openResponse(4));
        tracker.onFailure(new IllegalArgumentException("invalid stage value"));
        assertCounts(info, 4, 4, 0, 0);
        assertEquals(EsqlExecutionInfo.Cluster.Status.FAILED, cluster(info).getStatus());
        assertEquals(1, cluster(info).getFailures().size());
        assertFalse(info.isPartial());
    }

    private static EsqlExecutionInfo executionInfo() {
        var info = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        info.initCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "local", "sequence-a,sequence-b");
        return info;
    }

    private static OpenPointInTimeResponse openResponse(int shards) {
        return new OpenPointInTimeResponse(new BytesArray("snapshot"), shards, shards, 0, 0, SearchResponse.Clusters.EMPTY);
    }

    private static SearchResponse searchResponse(int total, int successful, int skipped, boolean timedOut, ShardSearchFailure... failures) {
        return new SearchResponse(
            SearchHits.EMPTY_WITHOUT_TOTAL_HITS,
            null,
            null,
            timedOut,
            null,
            null,
            1,
            null,
            total,
            successful,
            skipped,
            0,
            failures,
            SearchResponse.Clusters.EMPTY
        );
    }

    private static EsqlExecutionInfo.Cluster cluster(EsqlExecutionInfo info) {
        return info.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
    }

    private static void assertCounts(EsqlExecutionInfo info, int total, int successful, int skipped, int failed) {
        assertEquals(Integer.valueOf(total), cluster(info).getTotalShards());
        assertEquals(Integer.valueOf(successful), cluster(info).getSuccessfulShards());
        assertEquals(Integer.valueOf(skipped), cluster(info).getSkippedShards());
        assertEquals(Integer.valueOf(failed), cluster(info).getFailedShards());
    }
}
