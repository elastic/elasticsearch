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

package org.elasticsearch.indices.store;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

import java.io.File;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class IndicesStoreTests extends ElasticsearchIntegrationTest {
    private static final Settings SETTINGS = settingsBuilder().put("gateway.type", "local").build();

    @Test
    public void shardsCleanup() throws Exception {
        final String node_1 = cluster().startNode(SETTINGS);
        final String node_2 = cluster().startNode(SETTINGS);
        logger.info("--> creating index [test] with one shard and on replica");
        client().admin().indices().create(createIndexRequest("test")
                .settings(settingsBuilder().put("index.numberOfReplicas", 1).put("index.numberOfShards", 1))).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());


        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory(node_2, "test", 0).exists(), equalTo(true));

        logger.info("--> starting node server3");
        String node_3 = cluster().startNode(SETTINGS);

        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(waitForShardDeletion(node_3, "test", 0), equalTo(false));

        File server2Shard = shardDirectory(node_2, "test", 0);
        logger.info("--> stopping node node_2");
        cluster().stopRandomNode(TestCluster.nameFilter(node_2));
        assertThat(server2Shard.exists(), equalTo(true));

        logger.info("--> running cluster_health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        logger.info("--> making sure that shard and its replica exist on server1, server2 and server3");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(server2Shard.exists(), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));

        logger.info("--> starting node node_4");
        final String node_4 = cluster().startNode(SETTINGS);

        logger.info("--> running cluster_health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        logger.info("--> making sure that shard and its replica are allocated on server1 and server3 but not on server2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));
        assertThat(waitForShardDeletion(node_4, "test", 0), equalTo(false));
    }

    private File shardDirectory(String server, String index, int shard) {
        NodeEnvironment env = cluster().getInstance(NodeEnvironment.class, server);
        return env.shardLocations(new ShardId(index, shard))[0];
    }

    private boolean waitForShardDeletion(final String server, final  String index, final int shard) throws InterruptedException {
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                return !shardDirectory(server, index, shard).exists();
            }
        });
        return shardDirectory(server, index, shard).exists();
    }


}
