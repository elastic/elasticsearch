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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

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

}
