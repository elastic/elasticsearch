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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.ccr.action.TransportFollowStatsActionTests.createShardFollowTask;
import static org.hamcrest.Matchers.equalTo;

public class TransportFollowInfoActionTests extends ESTestCase {

    public void testGetFollowInfos() {
        ClusterState state = createCS(
            new String[] { "follower1", "follower2", "follower3", "index4" },
            new boolean[] { true, true, true, false },
            new boolean[] { true, true, false, false }
        );
        List<String> concreteIndices = Arrays.asList("follower1", "follower3");

        List<FollowerInfo> result = TransportFollowInfoAction.getFollowInfos(concreteIndices, state);
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getFollowerIndex(), equalTo("follower1"));
        assertThat(result.get(0).getStatus(), equalTo(Response.Status.ACTIVE));
        assertThat(result.get(1).getFollowerIndex(), equalTo("follower3"));
        assertThat(result.get(1).getStatus(), equalTo(Response.Status.PAUSED));
    }

    private static ClusterState createCS(String[] indices, boolean[] followerIndices, boolean[] statuses) {
        PersistentTasksCustomMetadata.Builder persistentTasks = PersistentTasksCustomMetadata.builder();
        Metadata.Builder mdBuilder = Metadata.builder();
        for (int i = 0; i < indices.length; i++) {
            String index = indices[i];
            boolean isFollowIndex = followerIndices[i];
            boolean active = statuses[i];

            IndexMetadata.Builder imdBuilder = IndexMetadata.builder(index)
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0);

            if (isFollowIndex) {
                imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());
                if (active) {
                    persistentTasks.addTask(
                        Integer.toString(i),
                        ShardFollowTask.NAME,
                        createShardFollowTask(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE)),
                        null
                    );
                }
            }
            mdBuilder.put(imdBuilder);
        }

        mdBuilder.putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasks.build());
        return ClusterState.builder(new ClusterName("_cluster")).metadata(mdBuilder.build()).build();
    }

}
