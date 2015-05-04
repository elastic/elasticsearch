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

package org.elasticsearch.stresstest.manyindices;

import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.List;

public class ManyNodesManyIndicesRecoveryStressTest {

    public static void main(String[] args) throws Exception {
        final int NUM_NODES = 40;
        final int NUM_INDICES = 100;
        final int NUM_DOCS = 2;
        final int FLUSH_AFTER = 1;

        final Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("transport.netty.connections_per_node.low", 0)
                .put("transport.netty.connections_per_node.med", 0)
                .put("transport.netty.connections_per_node.high", 1)
                .build();

        final Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .build();

        List<Node> nodes = Lists.newArrayList();
        for (int i = 0; i < NUM_NODES; i++) {
            nodes.add(NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put(nodeSettings).put("name", "node" + i)).node());
        }
        Client client = nodes.get(0).client();

        for (int index = 0; index < NUM_INDICES; index++) {
            String indexName = "index_" + index;
            System.out.println("--> Processing index [" + indexName + "]...");
            client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).execute().actionGet();

            boolean flushed = false;
            for (int doc = 0; doc < NUM_DOCS; doc++) {
                if (!flushed && doc > FLUSH_AFTER) {
                    flushed = true;
                    client.admin().indices().prepareFlush(indexName).execute().actionGet();
                }
                client.prepareIndex(indexName, "type1", Integer.toString(doc)).setSource("field", "value" + doc).execute().actionGet();
            }
            System.out.println("--> DONE index [" + indexName + "]");
        }

        System.out.println("--> Initiating shutdown");
        for (Node node : nodes) {
            node.close();
        }

        System.out.println("--> Waiting for all nodes to be closed...");
        while (true) {
            boolean allAreClosed = true;
            for (Node node : nodes) {
                if (!node.isClosed()) {
                    allAreClosed = false;
                    break;
                }
            }
            if (allAreClosed) {
                break;
            }
            Thread.sleep(100);
        }
        System.out.println("Waiting a bit for node lock to really be released?");
        Thread.sleep(5000);
        System.out.println("--> All nodes are closed, starting back...");

        nodes = Lists.newArrayList();
        for (int i = 0; i < NUM_NODES; i++) {
            nodes.add(NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put(nodeSettings).put("name", "node" + i)).node());
        }
        client = nodes.get(0).client();

        System.out.println("--> Waiting for green status");
        while (true) {
            ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
            if (clusterHealth.isTimedOut()) {
                System.err.println("--> cluster health timed out..., active shards [" + clusterHealth.getActiveShards() + "]");
            } else {
                break;
            }
        }

        System.out.println("Verifying counts...");
        for (int index = 0; index < NUM_INDICES; index++) {
            String indexName = "index_" + index;
            CountResponse count = client.prepareCount(indexName).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            if (count.getCount() != NUM_DOCS) {
                System.err.println("Wrong count value, expected [" + NUM_DOCS + "], got [" + count.getCount() + "] for index [" + indexName + "]");
            }
        }

        System.out.println("Test end");
        for (Node node : nodes) {
            node.close();
        }
    }
}