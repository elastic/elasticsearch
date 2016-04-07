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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for the cluster allocation explanation
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public final class ClusterAllocationExplainIT extends ESIntegTestCase {
    public void testDelayShards() throws Exception {
        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodesAsync(3).get();

        // Wait for all 3 nodes to be up
        logger.info("--> waiting for 3 nodes to be up");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                NodesStatsResponse resp = client().admin().cluster().prepareNodesStats().get();
                assertThat(resp.getNodes().length, equalTo(3));
            }
        });

        logger.info("--> creating 'test' index");
        prepareCreate("test").setSettings(Settings.settingsBuilder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1m")
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 5)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)).get();
        ensureGreen("test");

        logger.info("--> stopping a random node");
        assertTrue(internalCluster().stopRandomDataNode());

        ensureYellow("test");

        ClusterAllocationExplainResponse resp = client().admin().cluster().prepareAllocationExplain().useAnyUnassignedShard().get();
        ClusterAllocationExplanation cae = resp.getExplanation();
        assertThat(cae.getShard().getIndexName(), equalTo("test"));
        assertFalse(cae.isPrimary());
        assertFalse(cae.isAssigned());
        assertThat("expecting a remaining delay, got: " + cae.getRemainingDelayNanos(), cae.getRemainingDelayNanos(), greaterThan(0L));
    }
}
