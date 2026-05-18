/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TransportUnfollowActionTests extends ESTestCase {

    public void testUnfollow() {
        @FixForMultiProject(description = "ccr is not project aware")
        final ProjectId projectId = ProjectId.DEFAULT;
        final long settingsVersion = randomNonNegativeLong();
        IndexMetadata.Builder followerIndex = IndexMetadata.builder("follow_index")
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .settingsVersion(settingsVersion)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(IndexMetadata.State.CLOSE)
            .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());

        ClusterState current = ClusterState.builder(new ClusterName("cluster_name"))
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(followerIndex)).build())
            .build();
        ClusterState result = TransportUnfollowAction.unfollow("follow_index", current);

        IndexMetadata resultIMD = result.metadata().getProject(projectId).index("follow_index");
        assertThat(resultIMD.getSettings().get(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey()), nullValue());
        assertThat(resultIMD.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY), nullValue());
        assertThat(resultIMD.getSettingsVersion(), equalTo(settingsVersion + 1));
    }

    public void testUnfollowIndexOpen() {
        @FixForMultiProject(description = "ccr is not project aware")
        final ProjectId projectId = ProjectId.DEFAULT;
        IndexMetadata.Builder followerIndex = IndexMetadata.builder("follow_index")
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());

        ClusterState current = ClusterState.builder(new ClusterName("cluster_name"))
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(followerIndex)).build())
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> TransportUnfollowAction.unfollow("follow_index", current));
        assertThat(
            e.getMessage(),
            equalTo("cannot convert the follower index [follow_index] to a non-follower, because it has not been closed")
        );
    }

    public void testUnfollowRunningShardFollowTasks() {
        @FixForMultiProject(description = "ccr is not project aware")
        final ProjectId projectId = ProjectId.DEFAULT;
        IndexMetadata.Builder followerIndex = IndexMetadata.builder("follow_index")
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(IndexMetadata.State.CLOSE)
            .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());

        ShardFollowTask params = new ShardFollowTask(
            null,
            new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0),
            1024,
            1024,
            1,
            1,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            10240,
            ByteSizeValue.of(512, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            Collections.emptyMap()
        );
        PersistentTasksCustomMetadata.PersistentTask<?> task = new PersistentTasksCustomMetadata.PersistentTask<>(
            "id",
            ShardFollowTask.NAME,
            params,
            0,
            null
        );

        ClusterState current = ClusterState.builder(new ClusterName("cluster_name"))
            .metadata(
                Metadata.builder()
                    .put(
                        ProjectMetadata.builder(projectId)
                            .put(followerIndex)
                            .putCustom(
                                PersistentTasksCustomMetadata.TYPE,
                                new PersistentTasksCustomMetadata(0, Collections.singletonMap("id", task))
                            )
                    )
                    .build()
            )
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> TransportUnfollowAction.unfollow("follow_index", current));
        assertThat(
            e.getMessage(),
            equalTo("cannot convert the follower index [follow_index] to a non-follower, because it has not been paused")
        );
    }

    public void testUnfollowMissingIndex() {
        @FixForMultiProject(description = "ccr is not project aware")
        final ProjectId projectId = ProjectId.DEFAULT;
        IndexMetadata.Builder followerIndex = IndexMetadata.builder("follow_index")
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(IndexMetadata.State.CLOSE)
            .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());

        ClusterState current = ClusterState.builder(new ClusterName("cluster_name"))
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(followerIndex)).build())
            .build();
        expectThrows(IndexNotFoundException.class, () -> TransportUnfollowAction.unfollow("another_index", current));
    }

    public void testUnfollowNoneFollowIndex() {
        @FixForMultiProject(description = "ccr is not project aware")
        final ProjectId projectId = ProjectId.DEFAULT;
        IndexMetadata.Builder followerIndex = IndexMetadata.builder("follow_index")
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(IndexMetadata.State.CLOSE);

        ClusterState current = ClusterState.builder(new ClusterName("cluster_name"))
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(followerIndex)).build())
            .build();
        expectThrows(IllegalArgumentException.class, () -> TransportUnfollowAction.unfollow("follow_index", current));
    }

}
