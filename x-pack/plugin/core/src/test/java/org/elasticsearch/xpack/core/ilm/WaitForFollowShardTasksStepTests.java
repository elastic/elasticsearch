/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;

public class WaitForFollowShardTasksStepTests extends AbstractStepTestCase<WaitForFollowShardTasksStep> {

    @Override
    protected WaitForFollowShardTasksStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForFollowShardTasksStep(stepKey, nextStepKey, client);
    }

    @Override
    protected WaitForFollowShardTasksStep mutateInstance(WaitForFollowShardTasksStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return new WaitForFollowShardTasksStep(key, nextKey, instance.getClient());
    }

    @Override
    protected WaitForFollowShardTasksStep copyInstance(WaitForFollowShardTasksStep instance) {
        return new WaitForFollowShardTasksStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testConditionMet() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, 9, 9)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, 3, 3))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        createRandomInstance().evaluateCondition(
            Metadata.builder().put(indexMetadata, true).build(),
            indexMetadata.getIndex(),
            new AsyncWaitStep.Listener() {
                @Override
                public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                    conditionMetHolder[0] = conditionMet;
                    informationContextHolder[0] = informationContext;
                }

                @Override
                public void onFailure(Exception e) {
                    exceptionHolder[0] = e;
                }
            },
            MASTER_TIMEOUT
        );

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
        assertThat(exceptionHolder[0], nullValue());
    }

    public void testConditionNotMetShardsNotInSync() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, 9, 9)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, 8, 3))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        createRandomInstance().evaluateCondition(
            Metadata.builder().put(indexMetadata, true).build(),
            indexMetadata.getIndex(),
            new AsyncWaitStep.Listener() {
                @Override
                public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                    conditionMetHolder[0] = conditionMet;
                    informationContextHolder[0] = informationContext;
                }

                @Override
                public void onFailure(Exception e) {
                    exceptionHolder[0] = e;
                }
            },
            MASTER_TIMEOUT
        );

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
        assertThat(exceptionHolder[0], nullValue());
        WaitForFollowShardTasksStep.Info info = (WaitForFollowShardTasksStep.Info) informationContextHolder[0];
        assertThat(info.shardFollowTaskInfos().size(), equalTo(1));
        assertThat(info.shardFollowTaskInfos().get(0).shardId(), equalTo(1));
        assertThat(info.shardFollowTaskInfos().get(0).leaderGlobalCheckpoint(), equalTo(8L));
        assertThat(info.shardFollowTaskInfos().get(0).followerGlobalCheckpoint(), equalTo(3L));
    }

    public void testConditionNotMetNotAFollowerIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        createRandomInstance().evaluateCondition(
            Metadata.builder().put(indexMetadata, true).build(),
            indexMetadata.getIndex(),
            new AsyncWaitStep.Listener() {
                @Override
                public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                    conditionMetHolder[0] = conditionMet;
                    informationContextHolder[0] = informationContext;
                }

                @Override
                public void onFailure(Exception e) {
                    exceptionHolder[0] = e;
                }
            },
            MASTER_TIMEOUT
        );

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
        assertThat(exceptionHolder[0], nullValue());
        Mockito.verifyNoMoreInteractions(client);
    }

    private static ShardFollowNodeTaskStatus createShardFollowTaskStatus(int shardId, long leaderGCP, long followerGCP) {
        return new ShardFollowNodeTaskStatus(
            "remote",
            "leader-index",
            "follower-index",
            shardId,
            leaderGCP,
            -1,
            followerGCP,
            -1,
            -1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            Collections.emptyNavigableMap(),
            0,
            null
        );
    }

    private void mockFollowStatsCall(String expectedIndexName, List<FollowStatsAction.StatsResponse> statsResponses) {
        Mockito.doAnswer(invocationOnMock -> {
            FollowStatsAction.StatsRequest request = (FollowStatsAction.StatsRequest) invocationOnMock.getArguments()[1];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(expectedIndexName));

            @SuppressWarnings("unchecked")
            ActionListener<FollowStatsAction.StatsResponses> listener = (ActionListener<FollowStatsAction.StatsResponses>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(new FollowStatsAction.StatsResponses(List.of(), List.of(), statsResponses));
            return null;
        }).when(client).execute(Mockito.eq(FollowStatsAction.INSTANCE), Mockito.any(), Mockito.any());
    }
}
