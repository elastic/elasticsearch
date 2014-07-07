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
package org.elasticsearch.bwcompat;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 0, scope = ElasticsearchIntegrationTest.Scope.TEST, numClientNodes = 0, transportClientRatio = 0.0)
public class RecoveryBackwardsCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {


    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("action.admin.cluster.node.shutdown.delay", "10ms")
                .put("gateway.recover_after_nodes", 3)
                .put(BalancedShardsAllocator.SETTING_THRESHOLD, 1.1f).build(); // use less agressive settings
    }

    protected int minExternalNodes() {
        return 3;
    }

    protected int maxExternalNodes() {
        return 3;
    }

    @Test
    @LuceneTestCase.Slow
    public void testReusePeerRecovery() throws Exception {
        createIndex("test");
        logger.info("--> indexing docs");
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        client().admin().indices().prepareFlush().execute().actionGet();
        assertAllShardsOnNodes("test", backwardsCluster().backwardsNodePattern());

        logger.info("--> shutting down the nodes");
        logClusterState();
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.settingsBuilder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")).execute().actionGet();
        backwardsCluster().upgradeAllNodes();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.settingsBuilder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "all")).execute().actionGet();
        logger.info("Running Cluster Health");
        ensureGreen();
        countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);

        final int iters = randomIntBetween(1,2);
        for (int i = 0; i < iters; i++) {
            logger.info("--> shutting down the nodes iteration: {}", i);
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.settingsBuilder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")).execute().actionGet();
            backwardsCluster().fullRestartInternalCluster();
            logger.info("Running Cluster Health");
            ensureGreen();

            RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
            for (ShardRecoveryResponse response : recoveryResponse.shardResponses().get("test")) {
                RecoveryState recoveryState = response.recoveryState();
                if (!recoveryState.getPrimary()) {
                    RecoveryState.Index index = recoveryState.getIndex();
                    if (compatibilityVersion().onOrAfter(Version.V_1_3_0)) {
                        assertThat(index.toString(), index.recoveredByteCount(), equalTo(0l));
                        assertThat(index.toString(), index.reusedByteCount(), greaterThan(0l));
                        assertThat(index.toString(), index.reusedByteCount(), equalTo(index.totalByteCount()));
                        assertThat(index.toString(), index.recoveredFileCount(), equalTo(0));
                        assertThat(index.toString(), index.reusedFileCount(), equalTo(index.totalFileCount()));
                        assertThat(index.toString(), index.reusedFileCount(), greaterThan(0));
                        assertThat(index.toString(), index.reusedByteCount(), greaterThan(index.numberOfRecoveredBytes()));
                    } else {
                        assertThat(index.toString(), index.recoveredByteCount(), greaterThan(0l)); // before 1.3 we had to recover at least the segments files
                        assertThat(index.toString(), index.recoveredFileCount(), greaterThan(0));
                        assertThat(index.toString(), index.reusedByteCount(), greaterThan(0l));
                        assertThat(index.toString(), index.reusedByteCount(), greaterThan(index.numberOfRecoveredBytes()));
                    }
                    // TODO upgrade via optimize?
                }
            }
        }
    }
}
