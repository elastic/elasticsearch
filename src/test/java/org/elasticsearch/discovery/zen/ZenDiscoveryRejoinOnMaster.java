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

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
                .setTransientSettings(ImmutableSettings.builder().put(ZenDiscovery.REJOIN_ON_MASTER_GONE, false))
                .get();

        assertThat(zenDiscovery.isRejoinOnMasterGone(), is(false));
    }

    @Test
    public void testNoShardRelocationsOccurWhenElectedMasterNodeFails() throws Exception {
        Settings defaultSettings = ImmutableSettings.builder()
                .put("discovery.zen.fd.ping_timeout", "1s")
                .put("discovery.zen.fd.ping_retries", "1")
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
        boolean result = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                String current = internalCluster().getMasterName();
                return current != null && !current.equals(oldMaster);
            }
        });
        assertTrue(result);
        ensureSearchable("test");

        r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesAfterNewMaster = r.shardResponses().get("test").size();
        assertThat(numRecoveriesAfterNewMaster, equalTo(numRecoveriesBeforeNewMaster));
    }

}
