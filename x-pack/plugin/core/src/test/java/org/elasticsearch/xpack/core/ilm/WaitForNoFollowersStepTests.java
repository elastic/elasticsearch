/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;

import static org.elasticsearch.xpack.core.ilm.WaitForNoFollowersStep.CCR_LEASE_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WaitForNoFollowersStepTests extends AbstractStepTestCase<WaitForNoFollowersStep> {

    @Override
    protected WaitForNoFollowersStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return new WaitForNoFollowersStep(stepKey, nextStepKey, client);
    }

    @Override
    protected WaitForNoFollowersStep mutateInstance(WaitForNoFollowersStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(nextKey.getPhase(), nextKey.getAction(), nextKey.getName() + randomAlphaOfLength(5));
        }

        return new WaitForNoFollowersStep(key, nextKey, instance.getClient());
    }

    @Override
    protected WaitForNoFollowersStep copyInstance(WaitForNoFollowersStep instance) {
        return new WaitForNoFollowersStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testConditionMet() {
        WaitForNoFollowersStep step = createRandomInstance();

        String indexName = randomAlphaOfLengthBetween(5, 10);

        int numberOfShards = randomIntBetween(1, 100);
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(1, 10))
            .build();

        mockIndexStatsCall(indexName, randomIndexStats(false, numberOfShards));

        final SetOnce<Boolean> conditionMetHolder = new SetOnce<>();
        final SetOnce<ToXContentObject> stepInfoHolder = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject infomationContext) {
                conditionMetHolder.set(conditionMet);
                stepInfoHolder.set(infomationContext);
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not be called in this test, called with exception: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertTrue(conditionMetHolder.get());
        assertNull(stepInfoHolder.get());
    }

    public void testConditionNotMet() {
        WaitForNoFollowersStep step = createRandomInstance();

        String indexName = randomAlphaOfLengthBetween(5, 10);

        int numberOfShards = randomIntBetween(1, 100);
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(1, 10))
            .build();

        mockIndexStatsCall(indexName, randomIndexStats(true, numberOfShards));

        final SetOnce<Boolean> conditionMetHolder = new SetOnce<>();
        final SetOnce<ToXContentObject> stepInfoHolder = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject infomationContext) {
                conditionMetHolder.set(conditionMet);
                stepInfoHolder.set(infomationContext);
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not be called in this test, called with exception: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertFalse(conditionMetHolder.get());
        assertThat(
            Strings.toString(stepInfoHolder.get()),
            containsString("this index is a leader index; waiting for all following indices to cease following before proceeding")
        );
    }

    public void testNoShardStats() {
        WaitForNoFollowersStep step = createRandomInstance();

        String indexName = randomAlphaOfLengthBetween(5, 10);

        int numberOfShards = randomIntBetween(1, 100);
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(1, 10))
            .build();

        ShardStats sStats = new ShardStats(null, mockShardPath(), null, null, null, null);
        ShardStats[] shardStats = new ShardStats[1];
        shardStats[0] = sStats;
        mockIndexStatsCall(indexName, new IndexStats(indexName, "uuid", ClusterHealthStatus.GREEN, IndexMetadata.State.OPEN, shardStats));

        final SetOnce<Boolean> conditionMetHolder = new SetOnce<>();
        final SetOnce<ToXContentObject> stepInfoHolder = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject infomationContext) {
                conditionMetHolder.set(conditionMet);
                stepInfoHolder.set(infomationContext);
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not be called in this test, called with exception: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertTrue(conditionMetHolder.get());
        assertNull(stepInfoHolder.get());
    }

    public void testFailure() {
        WaitForNoFollowersStep step = createRandomInstance();

        String indexName = randomAlphaOfLengthBetween(5, 10);

        int numberOfShards = randomIntBetween(1, 100);
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(1, 10))
            .build();

        final Exception expectedException = new RuntimeException(randomAlphaOfLength(5));

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(expectedException);
            return null;
        }).when(indicesClient).stats(any(), any());

        final SetOnce<Exception> exceptionHolder = new SetOnce<>();
        step.evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject infomationContext) {
                fail(
                    "onResponse should not be called in this test, called with conditionMet: "
                        + conditionMet
                        + " and stepInfo: "
                        + Strings.toString(infomationContext)
                );
            }

            @Override
            public void onFailure(Exception e) {
                exceptionHolder.set(e);
            }
        }, MASTER_TIMEOUT);

        assertThat(exceptionHolder.get(), equalTo(expectedException));
    }

    private void mockIndexStatsCall(String expectedIndexName, IndexStats indexStats) {
        Mockito.doAnswer(invocationOnMock -> {
            IndicesStatsRequest request = (IndicesStatsRequest) invocationOnMock.getArguments()[0];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(expectedIndexName));

            @SuppressWarnings("unchecked")
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) invocationOnMock.getArguments()[1];

            // Trying to create a real IndicesStatsResponse requires setting up a ShardRouting, so just mock it
            IndicesStatsResponse response = mock(IndicesStatsResponse.class);
            when(response.getIndex(expectedIndexName)).thenReturn(indexStats);

            listener.onResponse(response);
            return null;
        }).when(indicesClient).stats(any(), any());
    }

    private IndexStats randomIndexStats(boolean isLeaderIndex, int numOfShards) {
        ShardStats[] shardStats = new ShardStats[numOfShards];
        for (int i = 0; i < numOfShards; i++) {
            shardStats[i] = randomShardStats(isLeaderIndex);
        }
        return new IndexStats(randomAlphaOfLength(5), randomAlphaOfLength(10), null, null, shardStats);
    }

    private ShardStats randomShardStats(boolean isLeaderIndex) {
        return new ShardStats(null, mockShardPath(), null, null, null, randomRetentionLeaseStats(isLeaderIndex));
    }

    private RetentionLeaseStats randomRetentionLeaseStats(boolean isLeaderIndex) {
        int numOfLeases = randomIntBetween(1, 10);

        ArrayList<RetentionLease> leases = new ArrayList<>();
        for (int i = 0; i < numOfLeases; i++) {
            leases.add(
                new RetentionLease(
                    randomAlphaOfLength(5),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    isLeaderIndex ? CCR_LEASE_KEY : randomAlphaOfLength(5)
                )
            );
        }
        return new RetentionLeaseStats(
            new RetentionLeases(randomLongBetween(1, Long.MAX_VALUE), randomLongBetween(1, Long.MAX_VALUE), leases)
        );
    }

    private ShardPath mockShardPath() {
        // Mock paths in a way that pass ShardPath constructor assertions
        final int shardId = randomIntBetween(0, 10);
        final Path getFileNameShardId = mock(Path.class);
        when(getFileNameShardId.toString()).thenReturn(Integer.toString(shardId));

        final String shardUuid = randomAlphaOfLength(5);
        final Path getFileNameShardUuid = mock(Path.class);
        when(getFileNameShardUuid.toString()).thenReturn(shardUuid);

        final Path getParent = mock(Path.class);
        when(getParent.getFileName()).thenReturn(getFileNameShardUuid);

        final Path path = mock(Path.class);
        when(path.getParent()).thenReturn(getParent);
        when(path.getFileName()).thenReturn(getFileNameShardId);

        // Mock paths for ShardPath#getRootDataPath()
        final Path getParentOfParent = mock(Path.class);
        when(getParent.getParent()).thenReturn(getParentOfParent);
        when(getParentOfParent.getParent()).thenReturn(mock(Path.class));

        return new ShardPath(false, path, path, new ShardId(randomAlphaOfLength(5), shardUuid, shardId));
    }
}
