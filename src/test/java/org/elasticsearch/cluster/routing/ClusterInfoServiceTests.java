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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@ElasticsearchIntegrationTest.ClusterScope(scope= ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes =0)
public class ClusterInfoServiceTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "1s")
                .build();
    }

    @Test
    public void testClusterInfoServiceCollectsInformation() throws Exception {
        createIndex("test");
        ensureGreen("test");
        Thread.sleep(2000); // wait 2 seconds for new information to be gathered
        InternalTestCluster internalTestCluster = internalCluster();
        // Get the cluster info service on the master node
        ClusterInfoService infoService = internalTestCluster.getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        ClusterInfo info = infoService.getClusterInfo();
        Map<String, DiskUsage> usages = info.getNodeDiskUsages();
        Map<String, Long> shardSizes = info.getShardSizes();
        assertNotNull(usages);
        assertNotNull(shardSizes);
        assertThat("some usages are populated", usages.values().size(), greaterThan(0));
        assertThat("some shard sizes are populated", shardSizes.values().size(), greaterThan(0));
        for (DiskUsage usage : usages.values()) {
            logger.info("--> usage: {}", usage);
            assertThat("usage has be retrieved", usage.getFreeBytes(), greaterThan(0L));
        }
        for (Long size : shardSizes.values()) {
            logger.info("--> shard size: {}", size);
            assertThat("shard size is greater than 0", size, greaterThan(0L));
        }
    }
}
