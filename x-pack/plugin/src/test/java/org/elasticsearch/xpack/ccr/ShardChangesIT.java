/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
/*

/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 *  [2017] Elasticsearch Incorporated. All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Elasticsearch Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Elasticsearch Incorporated.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.ccr.action.UnfollowIndexAction;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, transportClientRatio = 0)
public class ShardChangesIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal)  {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings(nodeOrdinal));
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Collections.singleton(TestSeedPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, CommonAnalysisPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    // Something like this will emulate what the xdrc persistent task will do for pulling
    // the changes:
    public void testGetOperationsBasedOnGlobalSequenceId() throws Exception {
        client().admin().indices().prepareCreate("index")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .get();

        client().prepareIndex("index", "doc", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "3").setSource("{}", XContentType.JSON).get();

        ShardStats shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        long globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(2L));

        ShardChangesAction.Request request = new ShardChangesAction.Request(shardStats.getShardRouting().shardId());
        request.setMinSeqNo(0L);
        request.setMaxSeqNo(globalCheckPoint);
        ShardChangesAction.Response response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().size(), equalTo(3));
        Translog.Index operation = (Translog.Index) response.getOperations().get(0);
        assertThat(operation.seqNo(), equalTo(0L));
        assertThat(operation.id(), equalTo("1"));

        operation = (Translog.Index) response.getOperations().get(1);
        assertThat(operation.seqNo(), equalTo(1L));
        assertThat(operation.id(), equalTo("2"));

        operation = (Translog.Index) response.getOperations().get(2);
        assertThat(operation.seqNo(), equalTo(2L));
        assertThat(operation.id(), equalTo("3"));

        client().prepareIndex("index", "doc", "3").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "4").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "5").setSource("{}", XContentType.JSON).get();

        shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(5L));

        request = new ShardChangesAction.Request(shardStats.getShardRouting().shardId());
        request.setMinSeqNo(3L);
        request.setMaxSeqNo(globalCheckPoint);
        response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().size(), equalTo(3));
        operation = (Translog.Index) response.getOperations().get(0);
        assertThat(operation.seqNo(), equalTo(3L));
        assertThat(operation.id(), equalTo("3"));

        operation = (Translog.Index) response.getOperations().get(1);
        assertThat(operation.seqNo(), equalTo(4L));
        assertThat(operation.id(), equalTo("4"));

        operation = (Translog.Index) response.getOperations().get(2);
        assertThat(operation.seqNo(), equalTo(5L));
        assertThat(operation.id(), equalTo("5"));
    }


    public void testFollowIndex() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);

        assertAcked(client().admin().indices().prepareCreate("index1")
                .setSettings(Settings.builder().put("index.number_of_shards", numberOfPrimaryShards)));
        assertAcked(client().admin().indices().prepareCreate("index2")
                .setSettings(Settings.builder().put("index.number_of_shards", numberOfPrimaryShards)));
        ensureGreen("index1", "index2");

        FollowExistingIndexAction.Request followRequest = new FollowExistingIndexAction.Request();
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowIndex("index2");
        client().execute(FollowExistingIndexAction.INSTANCE, followRequest).get();

        int numDocs = randomIntBetween(2, 64);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("index1", "doc").setSource("{}", XContentType.JSON).get();
        }

        final Map<ShardId, Long> numDocsPerShard = new HashMap<>();
        ShardStats[] shardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (ShardStats shardStat : shardStats) {
            if (shardStat.getShardRouting().primary()) {
                long value = shardStat.getStats().getIndexing().getTotal().getIndexCount() - 1;
                numDocsPerShard.put(shardStat.getShardRouting().shardId(), value);
            }
        }

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks().size(), equalTo(numberOfPrimaryShards));

            for (PersistentTasksCustomMetaData.PersistentTask<?> task : tasks.tasks()) {
                ShardFollowTask shardFollowTask = (ShardFollowTask) task.getParams();
                ShardFollowTask.Status status = (ShardFollowTask.Status) task.getStatus();
                assertThat(status, notNullValue());
                assertThat(status.getProcessedGlobalCheckpoint(), equalTo(numDocsPerShard.get(shardFollowTask.getLeaderShardId())));
            }
        });

        numDocs = randomIntBetween(2, 64);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("index1", "doc").setSource("{}", XContentType.JSON).get();
        }

        numDocsPerShard.clear();
        shardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (ShardStats shardStat : shardStats) {
            if (shardStat.getShardRouting().primary()) {
                long value = shardStat.getStats().getIndexing().getTotal().getIndexCount() - 1;
                numDocsPerShard.put(shardStat.getShardRouting().shardId(), value);
            }
        }

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks().size(), equalTo(numberOfPrimaryShards));

            for (PersistentTasksCustomMetaData.PersistentTask<?> task : tasks.tasks()) {
                ShardFollowTask shardFollowTask = (ShardFollowTask) task.getParams();
                ShardFollowTask.Status status = (ShardFollowTask.Status) task.getStatus();
                assertThat(status, notNullValue());
                assertThat(status.getProcessedGlobalCheckpoint(), equalTo(numDocsPerShard.get(shardFollowTask.getLeaderShardId())));
            }
        });

        UnfollowIndexAction.Request unfollowRequest = new UnfollowIndexAction.Request();
        unfollowRequest.setFollowIndex("index2");
        client().execute(UnfollowIndexAction.INSTANCE, unfollowRequest).get();

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks().size(), equalTo(0));
        });
    }

}
