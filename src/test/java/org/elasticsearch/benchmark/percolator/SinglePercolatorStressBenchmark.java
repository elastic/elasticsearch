/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.percolator;

import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class SinglePercolatorStressBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("cluster.routing.schedule", 200, TimeUnit.MILLISECONDS)
                .put("gateway.type", "none")
                .put(SETTING_NUMBER_OF_SHARDS, 2)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Node[] nodes = new Node[2];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }

        Node client = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        Client client1 = client.client();

        client1.admin().indices().create(createIndexRequest("test")).actionGet();
        Thread.sleep(1000);

        int COUNT = 200000;
        int QUERIES = 10;
        // register queries
        for (int i = 0; i < QUERIES; i++) {
            client1.prepareIndex("_percolator", "test", Integer.toString(i))
                    .setSource(jsonBuilder().startObject()
                            .field("query", termQuery("name", "value"))
                            .endObject())
                    .setRefresh(true)
                    .execute().actionGet();
        }

        StopWatch stopWatch = new StopWatch().start();
        System.out.println("Percolating [" + COUNT + "] ...");
        int i = 1;
        for (; i <= COUNT; i++) {
            PercolateResponse percolate = client1.preparePercolate("test", "type1").setSource(source(Integer.toString(i), "value"))
                    .execute().actionGet();
            if (percolate.getMatches().size() != QUERIES) {
                System.err.println("No matching number of queries");
            }
            if ((i % 10000) == 0) {
                System.out.println("Percolated " + i + " took " + stopWatch.stop().lastTaskTime());
                stopWatch.start();
            }
        }
        System.out.println("Percolation took " + stopWatch.totalTime() + ", TPS " + (((double) COUNT) / stopWatch.totalTime().secondsFrac()));

        client.close();

        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String id, String nameValue) throws IOException {
        long time = System.currentTimeMillis();
        return jsonBuilder().startObject().startObject("doc")
                .field("id", id)
                .field("numeric1", time)
                .field("numeric2", time)
                .field("numeric3", time)
                .field("numeric4", time)
                .field("numeric5", time)
                .field("numeric6", time)
                .field("numeric7", time)
                .field("numeric8", time)
                .field("numeric9", time)
                .field("numeric10", time)
                .field("name", nameValue)
                .endObject().endObject();
    }
}
