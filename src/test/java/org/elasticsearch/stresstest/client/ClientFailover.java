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

package org.elasticsearch.stresstest.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ClientFailover {

    public static void main(String[] args) throws Exception {
        Node[] nodes = new Node[3];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().node();
        }

        final TransportClient client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9301))
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9302));

        final AtomicBoolean done = new AtomicBoolean();
        final AtomicLong indexed = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread indexer = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        client.prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
                        indexed.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                latch.countDown();
            }
        });
        indexer.start();

        for (int i = 0; i < 100; i++) {
            int index = i % nodes.length;
            nodes[index].close();

            ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
            if (health.isTimedOut()) {
                System.err.println("timed out on health");
            }

            nodes[index] = NodeBuilder.nodeBuilder().node();

            health = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
            if (health.isTimedOut()) {
                System.err.println("timed out on health");
            }
        }

        latch.await();

        // TODO add verification to the number of indexed docs
    }
}
