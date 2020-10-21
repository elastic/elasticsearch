/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new WaitForFollowShardTasksStep(key, nextKey, instance.getClient());
    }

    @Override
    protected WaitForFollowShardTasksStep copyInstance(WaitForFollowShardTasksStep instance) {
        return new WaitForFollowShardTasksStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testConditionMet() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        List<FollowStatsAction.StatsResponse> statsResponses = Arrays.asList(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, 9, 9)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, 3, 3))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        createRandomInstance().evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(),
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
            }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
        assertThat(exceptionHolder[0], nullValue());
    }

    public void testConditionNotMetShardsNotInSync() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        List<FollowStatsAction.StatsResponse> statsResponses = Arrays.asList(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, 9, 9)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, 8, 3))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        createRandomInstance().evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(),
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
            }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
        assertThat(exceptionHolder[0], nullValue());
        WaitForFollowShardTasksStep.Info info = (WaitForFollowShardTasksStep.Info) informationContextHolder[0];
        assertThat(info.getShardFollowTaskInfos().size(), equalTo(1));
        assertThat(info.getShardFollowTaskInfos().get(0).getShardId(), equalTo(1));
        assertThat(info.getShardFollowTaskInfos().get(0).getLeaderGlobalCheckpoint(), equalTo(8L));
        assertThat(info.getShardFollowTaskInfos().get(0).getFollowerGlobalCheckpoint(), equalTo(3L));
    }

    public void testConditionNotMetNotAFollowerIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        createRandomInstance().evaluateCondition(Metadata.builder().put(indexMetadata, true).build(), indexMetadata.getIndex(),
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
            }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
        assertThat(exceptionHolder[0], nullValue());
        Mockito.verifyZeroInteractions(client);
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
            ActionListener<FollowStatsAction.StatsResponses> listener =
                (ActionListener<FollowStatsAction.StatsResponses>) invocationOnMock.getArguments()[2];
            listener.onResponse(new FollowStatsAction.StatsResponses(Collections.emptyList(), Collections.emptyList(), statsResponses));
            return null;
        }).when(client).execute(Mockito.eq(FollowStatsAction.INSTANCE), Mockito.any(), Mockito.any());
    }
}
