/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportFollowStatsActionTests extends ESTestCase {

    public void testFindFollowerIndicesFromShardFollowTasks() {
        PersistentTasksCustomMetaData.Builder persistentTasks = PersistentTasksCustomMetaData.builder()
            .addTask("1", ShardFollowTask.NAME, createShardFollowTask("abc"), null)
            .addTask("2", ShardFollowTask.NAME, createShardFollowTask("def"), null);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_cluster"))
            .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, persistentTasks.build()).build())
            .build();
        Set<String> result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, null);
        assertThat(result.size(), equalTo(2));
        assertThat(result.contains("abc"), is(true));
        assertThat(result.contains("def"), is(true));

        result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, new String[]{"def"});
        assertThat(result.size(), equalTo(1));
        assertThat(result.contains("def"), is(true));

        result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, new String[]{"ghi"});
        assertThat(result.size(), equalTo(0));
    }

    static ShardFollowTask createShardFollowTask(String followerIndex) {
        return new ShardFollowTask(
            null,
            new ShardId(followerIndex, "", 0),
            new ShardId("leader_index", "", 0),
            1024,
            1024,
            1,
            1,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            10240,
            new ByteSizeValue(512, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            Collections.emptyMap()
        );
    }

}
