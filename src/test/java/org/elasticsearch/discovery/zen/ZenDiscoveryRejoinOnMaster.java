/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.zen;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ZenDiscoveryRejoinOnMaster extends ElasticsearchIntegrationTest {

    @Test
    public void testChangeRejoinOnMasterOptionIsDynamic() throws Exception {
        Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "zen") // <-- To override the local setting if set externally
                .build();
        String nodeName = internalCluster().startNode(nodeSettings);
        ZenDiscovery zenDiscovery = (ZenDiscovery) internalCluster().getInstance(Discovery.class, nodeName);
        assertThat(zenDiscovery.isRejoinOnMasterGone(), is(true));

        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder().put(ZenDiscovery.SETTING_REJOIN_ON_MASTER_GONE, false))
                .get();

        assertThat(zenDiscovery.isRejoinOnMasterGone(), is(false));
    }

    @Test
    public void testNoShardRelocationsOccurWhenElectedMasterNodeFails() throws Exception {
        Settings defaultSettings = ImmutableSettings.builder()
                .put(FaultDetection.SETTING_PING_TIMEOUT, "1s")
                .put(FaultDetection.SETTING_PING_RETRIES, "1")
                .put("discovery.type", "zen")
                .build();

        Settings masterNodeSettings = ImmutableSettings.builder()
                .put("node.data", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, masterNodeSettings).get();
        Settings dateNodeSettings = ImmutableSettings.builder()
                .put("node.master", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, dateNodeSettings).get();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("4")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        createIndex("test");
        ensureSearchable("test");
        RecoveryResponse r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesBeforeNewMaster = r.shardResponses().get("test").size();

        final String oldMaster = internalCluster().getMasterName();
        internalCluster().stopCurrentMasterNode();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                String current = internalCluster().getMasterName();
                assertThat(current, notNullValue());
                assertThat(current, not(equalTo(oldMaster)));
            }
        });
        ensureSearchable("test");

        r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesAfterNewMaster = r.shardResponses().get("test").size();
        assertThat(numRecoveriesAfterNewMaster, equalTo(numRecoveriesBeforeNewMaster));
    }

}
