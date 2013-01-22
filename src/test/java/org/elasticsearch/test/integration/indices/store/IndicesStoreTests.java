/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.indices.store;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class IndicesStoreTests extends AbstractNodesTests {

    protected Client client1;

    @BeforeClass
    public void startNodes() {
        // The default (none) gateway cleans the shards on closing
        putDefaultSettings(settingsBuilder().put("gateway.type", "local"));
        startNode("server1");
        startNode("server2");
        client1 = getClient1();
    }

    @AfterClass
    public void closeNodes() {
        client1.close();
        closeAllNodes();
    }

    protected Client getClient1() {
        return client("server1");
    }

    @Test
    public void shardsCleanup() {
        try {
            client1.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception ex) {
            // Ignore
        }

        logger.info("--> creating index [test] with one shard and on replica");
        client1.admin().indices().create(createIndexRequest("test")
                .settings(settingsBuilder().put("index.numberOfReplicas", 1).put("index.numberOfShards", 1))).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.status());


        logger.info("--> making sure that shard and it's replica are allocated on server1 and server2");
        assertThat(shardDirectory("server1", "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory("server2", "test", 0).exists(), equalTo(true));

        logger.info("--> starting node server3");
        startNode("server3");

        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(shardDirectory("server3", "test", 0).exists(), equalTo(false));

        File server2Shard = shardDirectory("server2", "test", 0);
        logger.info("--> stopping node server2");
        closeNode("server2");
        assertThat(server2Shard.exists(), equalTo(true));

        logger.info("--> running cluster_health");
        clusterHealth = client1.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.status());

        logger.info("--> making sure that shard and it's replica exist on server1, server2 and server3");
        assertThat(shardDirectory("server1", "test", 0).exists(), equalTo(true));
        assertThat(server2Shard.exists(), equalTo(true));
        assertThat(shardDirectory("server3", "test", 0).exists(), equalTo(true));

        logger.info("--> starting node server2");
        startNode("server2");

        logger.info("--> running cluster_health");
        clusterHealth = client("server2").admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.status());

        logger.info("--> making sure that shard and it's replica are allocated on server1 and server3 but not on server2");
        assertThat(shardDirectory("server1", "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory("server2", "test", 0).exists(), equalTo(false));
        assertThat(shardDirectory("server3", "test", 0).exists(), equalTo(true));
    }

    private File shardDirectory(String server, String index, int shard) {
        InternalNode node = ((InternalNode) node(server));
        NodeEnvironment env = node.injector().getInstance(NodeEnvironment.class);
        return env.shardLocations(new ShardId(index, shard))[0];
    }


}
