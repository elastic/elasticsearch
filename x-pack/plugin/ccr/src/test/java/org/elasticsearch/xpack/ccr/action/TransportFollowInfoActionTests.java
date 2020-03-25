/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.ccr.action.TransportFollowStatsActionTests.createShardFollowTask;
import static org.hamcrest.Matchers.equalTo;

public class TransportFollowInfoActionTests extends ESTestCase {

    public void testGetFollowInfos() {
        ClusterState state = createCS(
            new String[] {"follower1", "follower2", "follower3", "index4"},
            new boolean[]{true, true, true, false},
            new boolean[]{true, true, false, false}
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
        PersistentTasksCustomMetaData.Builder persistentTasks = PersistentTasksCustomMetaData.builder();
        MetaData.Builder mdBuilder = MetaData.builder();
        for (int i = 0; i < indices.length; i++) {
            String index = indices[i];
            boolean isFollowIndex = followerIndices[i];
            boolean active = statuses[i];

            IndexMetaData.Builder imdBuilder = IndexMetaData.builder(index)
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0);

            if (isFollowIndex) {
                imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, new HashMap<>());
                if (active) {
                    persistentTasks.addTask(Integer.toString(i), ShardFollowTask.NAME,
                        createShardFollowTask(new Index(index, IndexMetaData.INDEX_UUID_NA_VALUE)), null);
                }
            }
            mdBuilder.put(imdBuilder);
        }

        mdBuilder.putCustom(PersistentTasksCustomMetaData.TYPE, persistentTasks.build());
        return ClusterState.builder(new ClusterName("_cluster"))
            .metaData(mdBuilder.build())
            .build();
    }

}
