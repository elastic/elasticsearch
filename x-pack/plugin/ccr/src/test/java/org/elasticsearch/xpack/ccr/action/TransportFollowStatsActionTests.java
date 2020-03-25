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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportFollowStatsActionTests extends ESTestCase {

    public void testFindFollowerIndicesFromShardFollowTasks() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        IndexMetaData index1 = IndexMetaData.builder("index1").settings(indexSettings).build();
        IndexMetaData index2 = IndexMetaData.builder("index2").settings(indexSettings).build();
        IndexMetaData index3 = IndexMetaData.builder("index3").settings(indexSettings).build();

        PersistentTasksCustomMetaData.Builder persistentTasks = PersistentTasksCustomMetaData.builder()
            .addTask("1", ShardFollowTask.NAME, createShardFollowTask(index1.getIndex()), null)
            .addTask("2", ShardFollowTask.NAME, createShardFollowTask(index2.getIndex()), null)
            .addTask("3", ShardFollowTask.NAME, createShardFollowTask(index3.getIndex()), null);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_cluster"))
            .metaData(MetaData.builder()
                .putCustom(PersistentTasksCustomMetaData.TYPE, persistentTasks.build())
                // only add index1 and index2
                .put(index1, false)
                .put(index2, false)
                .build())
            .build();
        Set<String> result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState, null);
        assertThat(result.size(), equalTo(2));
        assertThat(result.contains(index1.getIndex().getName()), is(true));
        assertThat(result.contains(index2.getIndex().getName()), is(true));

        result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState,
            new String[]{index2.getIndex().getName()});
        assertThat(result.size(), equalTo(1));
        assertThat(result.contains(index2.getIndex().getName()), is(true));

        result = TransportFollowStatsAction.findFollowerIndicesFromShardFollowTasks(clusterState,
            new String[]{index3.getIndex().getName()});
        assertThat(result.size(), equalTo(0));
    }

    static ShardFollowTask createShardFollowTask(Index followerIndex) {
        return new ShardFollowTask(
            null,
            new ShardId(followerIndex, 0),
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
