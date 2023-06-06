/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.rest.RestSearchableSnapshotsStatsAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class SearchableSnapshotsStatsResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        for (int i = 0; i < randomIntBetween(10, 50); i++) {
            final SearchableSnapshotsStatsResponse testInstance = createTestInstance();
            final SearchableSnapshotsStatsResponse deserializedInstance = copyWriteable(
                testInstance,
                writableRegistry(),
                SearchableSnapshotsStatsResponse::new,
                TransportVersion.CURRENT
            );
            assertEqualInstances(testInstance, deserializedInstance);
        }
    }

    public void testLevelValidation() {
        RestSearchableSnapshotsStatsAction action = new RestSearchableSnapshotsStatsAction();
        final HashMap<String, String> params = new HashMap<>();
        params.put("level", ClusterStatsLevel.CLUSTER.getLevel());

        // cluster is valid
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_searchable_snapshots/stats")
            .withParams(params)
            .build();
        action.prepareRequest(request, mock(NodeClient.class));

        // indices is valid
        params.put("level", ClusterStatsLevel.INDICES.getLevel());
        request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_searchable_snapshots/stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        // shards is valid
        params.put("level", ClusterStatsLevel.SHARDS.getLevel());
        request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_searchable_snapshots/stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        params.put("level", NodeStatsLevel.NODE.getLevel());
        final RestRequest invalidLevelRequest1 = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats")
            .withParams(params)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(invalidLevelRequest1, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("level parameter must be one of [cluster] or [indices] or [shards] but was [node]")));

        params.put("level", "invalid");
        final RestRequest invalidLevelRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats")
            .withParams(params)
            .build();

        e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(invalidLevelRequest, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("level parameter must be one of [cluster] or [indices] or [shards] but was [invalid]")));
    }

    private void assertEqualInstances(SearchableSnapshotsStatsResponse expected, SearchableSnapshotsStatsResponse actual) {
        assertThat(actual.getTotalShards(), equalTo(expected.getTotalShards()));
        assertThat(actual.getSuccessfulShards(), equalTo(expected.getSuccessfulShards()));
        assertThat(actual.getFailedShards(), equalTo(expected.getFailedShards()));
        DefaultShardOperationFailedException[] originalFailures = expected.getShardFailures();
        DefaultShardOperationFailedException[] parsedFailures = actual.getShardFailures();
        assertThat(originalFailures.length, equalTo(parsedFailures.length));
        for (int i = 0; i < originalFailures.length; i++) {
            assertThat(originalFailures[i].index(), equalTo(parsedFailures[i].index()));
            assertThat(originalFailures[i].shardId(), equalTo(parsedFailures[i].shardId()));
            assertThat(originalFailures[i].status(), equalTo(parsedFailures[i].status()));
        }
        assertThat(actual.getStats(), equalTo(expected.getStats()));
    }

    private SearchableSnapshotsStatsResponse createTestInstance() {
        final int totalShards = randomIntBetween(0, 20);
        final int successfulShards = totalShards > 0 ? randomIntBetween(0, totalShards) : 0;
        final int failedShards = totalShards - successfulShards;

        final String indexName = randomAlphaOfLength(10);
        final int replicas = randomIntBetween(0, 2);
        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        final IndexId indexId = new IndexId(randomAlphaOfLength(5), randomAlphaOfLength(5));

        final List<SearchableSnapshotShardStats> shardStats = new ArrayList<>();
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

        for (int i = 0; i < totalShards; i++) {
            if (i < successfulShards) {
                shardStats.add(createSearchableSnapshotShardStats(indexName, i, true, snapshotId, indexId));
                for (int j = 0; j < replicas; j++) {
                    shardStats.add(createSearchableSnapshotShardStats(indexName, i, false, snapshotId, indexId));
                }
            } else {
                shardFailures.add(new DefaultShardOperationFailedException(indexName, i, new Exception()));
            }
        }

        return new SearchableSnapshotsStatsResponse(shardStats, totalShards, successfulShards, failedShards, shardFailures);
    }

    private static SearchableSnapshotShardStats createSearchableSnapshotShardStats(
        String index,
        int shardId,
        boolean primary,
        SnapshotId snapshotId,
        IndexId indexId
    ) {
        final ShardRouting shardRouting = newShardRouting(index, shardId, randomAlphaOfLength(5), primary, ShardRoutingState.STARTED);
        final List<SearchableSnapshotShardStats.CacheIndexInputStats> inputStats = new ArrayList<>();
        for (int j = 0; j < randomInt(10); j++) {
            inputStats.add(randomCacheIndexInputStats());
        }
        return new SearchableSnapshotShardStats(shardRouting, snapshotId, indexId, inputStats);
    }

    private static SearchableSnapshotShardStats.CacheIndexInputStats randomCacheIndexInputStats() {
        return new SearchableSnapshotShardStats.CacheIndexInputStats(
            randomAlphaOfLength(10),
            randomNonNegativeLong(),
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomCounter(),
            randomCounter(),
            randomCounter(),
            randomCounter(),
            randomCounter(),
            randomCounter(),
            randomCounter(),
            randomCounter(),
            randomTimedCounter(),
            randomTimedCounter(),
            randomTimedCounter(),
            randomCounter(),
            randomCounter(),
            randomNonNegativeLong()
        );
    }

    private static SearchableSnapshotShardStats.Counter randomCounter() {
        return new SearchableSnapshotShardStats.Counter(randomLong(), randomLong(), randomLong(), randomLong());
    }

    private static SearchableSnapshotShardStats.TimedCounter randomTimedCounter() {
        return new SearchableSnapshotShardStats.TimedCounter(randomLong(), randomLong(), randomLong(), randomLong(), randomLong());
    }
}
