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
package org.elasticsearch.gateway;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESBackcompatTestCase;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST, numClientNodes = 0, transportClientRatio = 0.0)
public class RecoveryBackwardsCompatibilityIT extends ESBackcompatTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("gateway.recover_after_nodes", 2).build();
    }

    @Override
    protected int minExternalNodes() {
        return 2;
    }

    @Override
    protected int maxExternalNodes() {
        return 3;
    }

    public void testReusePeerRecovery() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings())
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)));
        logger.info("--> indexing docs");
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();

        logger.info("--> bump number of replicas from 0 to 1");
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1").build()).get();
        ensureGreen();

        assertAllShardsOnNodes("test", backwardsCluster().backwardsNodePattern());

        logger.info("--> upgrade cluster");
        logClusterState();
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")).execute().actionGet();
        backwardsCluster().upgradeAllNodes();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")).execute().actionGet();
        ensureGreen();

        countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").setDetailed(true).get();
        HashMap<String, String> map = new HashMap<>();
        map.put("details", "true");
        final ToXContent.Params params = new ToXContent.MapParams(map);
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            final String recoverStateAsJSON = XContentHelper.toString(recoveryState, params);
            if (!recoveryState.getPrimary()) {
                RecoveryState.Index index = recoveryState.getIndex();
                assertThat(recoverStateAsJSON, index.recoveredBytes(), equalTo(0L));
                assertThat(recoverStateAsJSON, index.reusedBytes(), greaterThan(0L));
                assertThat(recoverStateAsJSON, index.reusedBytes(), equalTo(index.totalBytes()));
                assertThat(recoverStateAsJSON, index.recoveredFileCount(), equalTo(0));
                assertThat(recoverStateAsJSON, index.reusedFileCount(), equalTo(index.totalFileCount()));
                assertThat(recoverStateAsJSON, index.reusedFileCount(), greaterThan(0));
                assertThat(recoverStateAsJSON, index.recoveredBytesPercent(), equalTo(100.f));
                assertThat(recoverStateAsJSON, index.recoveredFilesPercent(), equalTo(100.f));
                assertThat(recoverStateAsJSON, index.reusedBytes(), greaterThan(index.recoveredBytes()));
                // TODO upgrade via optimize?
            }
        }
    }
}
