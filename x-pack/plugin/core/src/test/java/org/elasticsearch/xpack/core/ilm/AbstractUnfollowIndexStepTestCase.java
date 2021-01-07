/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractUnfollowIndexStepTestCase<T extends AbstractUnfollowIndexStep> extends AbstractStepTestCase<T> {

    @Override
    protected final T createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return newInstance(stepKey, nextStepKey);
    }

    @Override
    protected final T mutateInstance(T instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return newInstance(key, nextKey);
    }

    @Override
    protected final T copyInstance(T instance) {
        return newInstance(instance.getKey(), instance.getNextStepKey());
    }

    public final void testNotAFollowerIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        T step = newInstance(randomStepKey(), randomStepKey());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        step.performAction(indexMetadata, null, null, new AsyncActionStep.Listener() {
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

        T step = newInstance(randomStepKey(), randomStepKey());

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
        T step = newInstance(randomStepKey(), randomStepKey());

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

    protected static ClusterState setupClusterStateWithFollowingIndex(IndexMetadata followerIndex) {
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

    protected abstract T newInstance(Step.StepKey key, Step.StepKey nextKey);
}
