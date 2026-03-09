/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.transport.RemoteTransportException;
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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WaitForIndexingCompleteStepTests extends AbstractStepTestCase<WaitForIndexingCompleteStep> {

    @Override
    protected WaitForIndexingCompleteStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForIndexingCompleteStep(stepKey, nextStepKey, client);
    }

    @Override
    protected WaitForIndexingCompleteStep mutateInstance(WaitForIndexingCompleteStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return new WaitForIndexingCompleteStep(key, nextKey, instance.getClient());
    }

    @Override
    protected WaitForIndexingCompleteStep copyInstance(WaitForIndexingCompleteStep instance) {
        return new WaitForIndexingCompleteStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testConditionMet() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
    }

    public void testConditionMetNotAFollowerIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
        Mockito.verifyNoMoreInteractions(projectClient);
    }

    public void testConditionNotMet() {
        Settings.Builder indexSettings = settings(IndexVersion.current());
        if (randomBoolean()) {
            indexSettings.put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "false");
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(indexSettings)
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, null))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
        WaitForIndexingCompleteStep.IndexingNotCompleteInfo info =
            (WaitForIndexingCompleteStep.IndexingNotCompleteInfo) informationContextHolder[0];
        assertThat(
            info.getMessage(),
            equalTo(
                "waiting for the [index.lifecycle.indexing_complete] setting to be set to "
                    + "true on the leader index, it is currently [false]"
            )
        );
    }

    public void testConditionMetWhenAllShardFollowTasksFailedWithIndexNotFound() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        ElasticsearchException fatalException = new IndexNotFoundException("leader-index");
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, fatalException)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, fatalException))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
    }

    public void testConditionNotMetWhenSomeShardFollowTasksFailed() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        ElasticsearchException fatalException = new IndexNotFoundException("leader-index");
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, fatalException)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, null))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
    }

    public void testConditionMetWhenNoShardFollowTasksFound() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        mockFollowStatsCallFailure(indexMetadata.getIndex().getName(), new ResourceNotFoundException("no tasks"));

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
    }

    public void testConditionNotMetOnFollowStatsError() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        mockFollowStatsCallFailure(indexMetadata.getIndex().getName(), new ElasticsearchException("something went wrong"));

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
    }

    public void testConditionNotMetWhenEmptyStatsResponse() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        mockFollowStatsCall(indexMetadata.getIndex().getName(), List.of(), List.of(), List.of());

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
    }

    public void testConditionNotMetWhenPartialNodeFailures() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        ElasticsearchException fatalException = new IndexNotFoundException("leader-index");
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, fatalException))
        );
        List<FailedNodeException> nodeFailures = List.of(new FailedNodeException("node-1", "node unreachable", new Exception()));
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses, List.of(), nodeFailures);

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
    }

    public void testConditionNotMetWhenPartialTaskFailures() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        ElasticsearchException fatalException = new IndexNotFoundException("leader-index");
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, fatalException))
        );
        List<TaskOperationFailure> taskFailures = List.of(new TaskOperationFailure("node-1", 1, new Exception("task failed")));
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses, taskFailures, List.of());

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
    }

    public void testConditionNotMetWhenAllTasksFailedWithNonIndexNotFoundException() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();

        ElasticsearchException fatalException = new ElasticsearchException("some unrelated fatal error");
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, fatalException)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, fatalException))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
    }

    public void testConditionMetWhenIndexNotFoundWrappedInRemoteTransportException() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ElasticsearchException fatalException = new RemoteTransportException("error on remote", new IndexNotFoundException("leader-index"));
        List<FollowStatsAction.StatsResponse> statsResponses = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, fatalException))
        );
        mockFollowStatsCall(indexMetadata.getIndex().getName(), statsResponses);

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        boolean[] conditionMetHolder = new boolean[1];
        ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        step.evaluateCondition(state, indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        }, MASTER_TIMEOUT);

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
    }

    public void testAllFollowTasksFailedWithIndexNotFound() {
        IndexNotFoundException indexNotFound = new IndexNotFoundException("leader-index");

        List<FollowStatsAction.StatsResponse> allFailedWithIndexNotFound = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, indexNotFound)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, indexNotFound))
        );
        assertThat(
            WaitForIndexingCompleteStep.allFollowTasksFailedWithIndexNotFound(
                new FollowStatsAction.StatsResponses(List.of(), List.of(), allFailedWithIndexNotFound)
            ),
            is(true)
        );

        ElasticsearchException wrappedIndexNotFound = new RemoteTransportException("error", indexNotFound);
        List<FollowStatsAction.StatsResponse> allFailedWrapped = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, wrappedIndexNotFound)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, wrappedIndexNotFound))
        );
        assertThat(
            WaitForIndexingCompleteStep.allFollowTasksFailedWithIndexNotFound(
                new FollowStatsAction.StatsResponses(List.of(), List.of(), allFailedWrapped)
            ),
            is(true)
        );

        ElasticsearchException otherException = new ElasticsearchException("some other error");
        List<FollowStatsAction.StatsResponse> allFailedOther = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, otherException)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, otherException))
        );
        assertThat(
            WaitForIndexingCompleteStep.allFollowTasksFailedWithIndexNotFound(
                new FollowStatsAction.StatsResponses(List.of(), List.of(), allFailedOther)
            ),
            is(false)
        );

        List<FollowStatsAction.StatsResponse> someFailedIndexNotFound = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, indexNotFound)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, null))
        );
        assertThat(
            WaitForIndexingCompleteStep.allFollowTasksFailedWithIndexNotFound(
                new FollowStatsAction.StatsResponses(List.of(), List.of(), someFailedIndexNotFound)
            ),
            is(false)
        );

        List<FollowStatsAction.StatsResponse> noneFailed = List.of(
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(0, null)),
            new FollowStatsAction.StatsResponse(createShardFollowTaskStatus(1, null))
        );
        assertThat(
            WaitForIndexingCompleteStep.allFollowTasksFailedWithIndexNotFound(
                new FollowStatsAction.StatsResponses(List.of(), List.of(), noneFailed)
            ),
            is(false)
        );

        assertThat(
            WaitForIndexingCompleteStep.allFollowTasksFailedWithIndexNotFound(
                new FollowStatsAction.StatsResponses(List.of(), List.of(), List.of())
            ),
            is(false)
        );
    }

    private static ShardFollowNodeTaskStatus createShardFollowTaskStatus(int shardId, ElasticsearchException fatalException) {
        return new ShardFollowNodeTaskStatus(
            "remote",
            "leader-index",
            "follower-index",
            shardId,
            0,
            -1,
            0,
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
            fatalException
        );
    }

    private void mockFollowStatsCall(String expectedIndexName, List<FollowStatsAction.StatsResponse> statsResponses) {
        mockFollowStatsCall(expectedIndexName, statsResponses, List.of(), List.of());
    }

    private void mockFollowStatsCall(
        String expectedIndexName,
        List<FollowStatsAction.StatsResponse> statsResponses,
        List<TaskOperationFailure> taskFailures,
        List<FailedNodeException> nodeFailures
    ) {
        Mockito.doAnswer(invocationOnMock -> {
            FollowStatsAction.StatsRequest request = (FollowStatsAction.StatsRequest) invocationOnMock.getArguments()[1];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(expectedIndexName));

            @SuppressWarnings("unchecked")
            ActionListener<FollowStatsAction.StatsResponses> listener = (ActionListener<FollowStatsAction.StatsResponses>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(new FollowStatsAction.StatsResponses(taskFailures, nodeFailures, statsResponses));
            return null;
        }).when(projectClient).execute(Mockito.eq(FollowStatsAction.INSTANCE), Mockito.any(), Mockito.any());
    }

    private void mockFollowStatsCallFailure(String expectedIndexName, Exception exception) {
        Mockito.doAnswer(invocationOnMock -> {
            FollowStatsAction.StatsRequest request = (FollowStatsAction.StatsRequest) invocationOnMock.getArguments()[1];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(expectedIndexName));

            @SuppressWarnings("unchecked")
            ActionListener<FollowStatsAction.StatsResponses> listener = (ActionListener<FollowStatsAction.StatsResponses>) invocationOnMock
                .getArguments()[2];
            listener.onFailure(exception);
            return null;
        }).when(projectClient).execute(Mockito.eq(FollowStatsAction.INSTANCE), Mockito.any(), Mockito.any());
    }
}
