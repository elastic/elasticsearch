/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PauseFollowerIndexStepTests extends AbstractUnfollowIndexStepTestCase<PauseFollowerIndexStep> {

    @Override
    protected PauseFollowerIndexStep newInstance(Step.StepKey key, Step.StepKey nextKey) {
        return new PauseFollowerIndexStep(key, nextKey, client);
    }

    public void testPauseFollowingIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = setupClusterStateWithFollowingIndex(indexMetadata);

        Mockito.doAnswer(invocation -> {
            PauseFollowAction.Request request = (PauseFollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowIndex(), equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        PauseFollowerIndexStep step = new PauseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], is(true));
        assertThat(failure[0], nullValue());
    }

    public void testRequestNotAcknowledged() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = setupClusterStateWithFollowingIndex(indexMetadata);

        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.FALSE);
            return null;
        }).when(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        PauseFollowerIndexStep step = new PauseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], nullValue());
        assertThat(failure[0], notNullValue());
        assertThat(failure[0].getMessage(), is("pause follow request failed to be acknowledged"));
    }

    public void testPauseFollowingIndexFailed() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = setupClusterStateWithFollowingIndex(indexMetadata);

        // Mock pause follow api call:
        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            PauseFollowAction.Request request = (PauseFollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowIndex(), equalTo("follower-index"));
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onFailure(error);
            return null;
        }).when(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        PauseFollowerIndexStep step = new PauseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], nullValue());
        assertThat(failure[0], sameInstance(error));
        Mockito.verify(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(client);
    }

    public final void testNoShardFollowPersistentTasks() {
        IndexMetadata indexMetadata = IndexMetadata.builder("managed-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        PersistentTasksCustomMetadata.Builder emptyPersistentTasks = PersistentTasksCustomMetadata.builder();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_cluster"))
            .metadata(Metadata.builder()
                .putCustom(PersistentTasksCustomMetadata.TYPE, emptyPersistentTasks.build())
                .put(indexMetadata, false)
                .build())
            .build();

        PauseFollowerIndexStep step = newInstance(randomStepKey(), randomStepKey());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], is(true));
        assertThat(failure[0], nullValue());
        Mockito.verifyZeroInteractions(client);
    }

    public final void testNoShardFollowTasksForManagedIndex() {
        IndexMetadata managedIndex = IndexMetadata.builder("managed-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexMetadata followerIndex = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = setupClusterStateWithFollowingIndex(followerIndex);
        // add the managed index to the cluster state too
        clusterState = ClusterState.builder(clusterState).metadata(Metadata.builder().put(managedIndex, false).build()).build();
        PauseFollowerIndexStep step = newInstance(randomStepKey(), randomStepKey());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        step.performAction(managedIndex, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], is(true));
        assertThat(failure[0], nullValue());
        Mockito.verifyZeroInteractions(client);
    }

    private static ClusterState setupClusterStateWithFollowingIndex(IndexMetadata followerIndex) {
        PersistentTasksCustomMetadata.Builder persistentTasks = PersistentTasksCustomMetadata.builder()
            .addTask("1", ShardFollowTask.NAME, new ShardFollowTask(
                null,
                new ShardId(followerIndex.getIndex(), 0),
                new ShardId("leader_index", "", 0),
                1024,
                1024,
                1,
                1,
                new ByteSizeValue(32, ByteSizeUnit.MB),
                new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
                10240,
                new ByteSizeValue(512, ByteSizeUnit.MB),
                TimeValue.timeValueMillis(10),
                TimeValue.timeValueMillis(10),
                Collections.emptyMap()
            ), null);

        return ClusterState.builder(new ClusterName("_cluster"))
            .metadata(Metadata.builder()
                .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks.build())
                .put(followerIndex, false)
                .build())
            .build();
    }

}
