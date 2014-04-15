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

package org.elasticsearch.stresstest.rollingrestart;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
public class QuickRollingRestartStressTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");

        Random random = new Random();

        Settings settings = ImmutableSettings.settingsBuilder().build();

        Node[] nodes = new Node[5];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }

        Node client = NodeBuilder.nodeBuilder().client(true).node();

        long COUNT;
        if (client.client().admin().indices().prepareExists("test").execute().actionGet().isExists()) {
            ClusterHealthResponse clusterHealthResponse = client.client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                throw new ElasticsearchException("failed to wait for green state on startup...");
            }
            COUNT = client.client().prepareCount().execute().actionGet().getCount();
            System.out.println("--> existing index, count [" + COUNT + "]");
        } else {
            COUNT = SizeValue.parseSizeValue("100k").singles();
            System.out.println("--> indexing data...");
            for (long i = 0; i < COUNT; i++) {
                client.client().prepareIndex("test", "type", Long.toString(i))
                        .setSource("date", new Date(), "data", RandomStrings.randomAsciiOfLength(random, 10000))
                        .execute().actionGet();
            }
            System.out.println("--> done indexing data [" + COUNT + "]");
            client.client().admin().indices().prepareRefresh().execute().actionGet();
            for (int i = 0; i < 10; i++) {
                long count = client.client().prepareCount().execute().actionGet().getCount();
                if (COUNT != count) {
                    System.err.println("--> the indexed docs do not match the count..., got [" + count + "], expected [" + COUNT + "]");
                }
            }
        }

        final int ROLLING_RESTARTS = 100;
        System.out.println("--> starting rolling restarts [" + ROLLING_RESTARTS + "]");
        for (int rollingRestart = 0; rollingRestart < ROLLING_RESTARTS; rollingRestart++) {
            System.out.println("--> doing rolling restart [" + rollingRestart + "]...");
            int nodeId = ThreadLocalRandom.current().nextInt();
            for (int i = 0; i < nodes.length; i++) {
                int nodeIdx = Math.abs(nodeId++) % nodes.length;
                nodes[nodeIdx].close();
                nodes[nodeIdx] = NodeBuilder.nodeBuilder().settings(settings).node();
            }
            System.out.println("--> done rolling restart [" + rollingRestart + "]");

            System.out.println("--> waiting for green state now...");
            ClusterHealthResponse clusterHealthResponse = client.client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForRelocatingShards(0).setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> timed out waiting for green state...");
                ClusterState state = client.client().admin().cluster().prepareState().execute().actionGet().getState();
                System.out.println(state.nodes().prettyPrint());
                System.out.println(state.routingTable().prettyPrint());
                System.out.println(state.routingNodes().prettyPrint());
                throw new ElasticsearchException("timed out waiting for green state");
            } else {
                System.out.println("--> got green status");
            }

            System.out.println("--> checking data [" + rollingRestart + "]....");
            boolean failed = false;
            for (int i = 0; i < 10; i++) {
                long count = client.client().prepareCount().execute().actionGet().getCount();
                if (COUNT != count) {
                    failed = true;
                    System.err.println("--> ERROR the indexed docs do not match the count..., got [" + count + "], expected [" + COUNT + "]");
                }
            }
            if (!failed) {
                System.out.println("--> count verified");
            }
        }

        System.out.println("--> shutting down...");
        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }
}
