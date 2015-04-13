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
package org.elasticsearch.benchmark.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.percolator.PercolatorService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class PercolatorStressBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("cluster.routing.schedule", 200, TimeUnit.MILLISECONDS)
                .put(SETTING_NUMBER_OF_SHARDS, 4)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }

        Node clientNode = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();
        Client client = clientNode.client();

        client.admin().indices().create(createIndexRequest("test")).actionGet();
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth("test")
                .setWaitForGreenStatus()
                .execute().actionGet();
        if (healthResponse.isTimedOut()) {
            System.err.println("Quiting, because cluster health requested timed out...");
            return;
        } else if (healthResponse.getStatus() != ClusterHealthStatus.GREEN) {
            System.err.println("Quiting, because cluster state isn't green...");
            return;
        }

        int COUNT = 200000;
        int QUERIES = 100;
        int TERM_QUERIES = QUERIES / 2;
        int RANGE_QUERIES = QUERIES - TERM_QUERIES;

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("numeric1", 1).endObject()).execute().actionGet();

        // register queries
        int i = 0;
        for (; i < TERM_QUERIES; i++) {
            client.prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject()
                            .field("query", termQuery("name", "value"))
                            .endObject())
                    .execute().actionGet();
        }

        int[] numbers = new int[RANGE_QUERIES];
        for (; i < QUERIES; i++) {
            client.prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject()
                            .field("query", rangeQuery("numeric1").from(i).to(i))
                            .endObject())
                    .execute().actionGet();
            numbers[i - TERM_QUERIES] = i;
        }

        StopWatch stopWatch = new StopWatch().start();
        System.out.println("Percolating [" + COUNT + "] ...");
        for (i = 1; i <= COUNT; i++) {
            XContentBuilder source;
            int expectedMatches;
            if (i % 2 == 0) {
                source = source(Integer.toString(i), "value");
                expectedMatches = TERM_QUERIES;
            } else {
                int number = numbers[i % RANGE_QUERIES];
                source = source(Integer.toString(i), number);
                expectedMatches = 1;
            }
            PercolateResponse percolate = client.preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setSource(source)
                    .execute().actionGet();
            if (percolate.getMatches().length != expectedMatches) {
                System.err.println("No matching number of queries");
            }

            if ((i % 10000) == 0) {
                System.out.println("Percolated " + i + " took " + stopWatch.stop().lastTaskTime());
                stopWatch.start();
            }
        }
        System.out.println("Percolation took " + stopWatch.totalTime() + ", TPS " + (((double) COUNT) / stopWatch.totalTime().secondsFrac()));

        clientNode.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String id, String nameValue) throws IOException {
        return jsonBuilder().startObject().startObject("doc")
                .field("id", id)
                .field("name", nameValue)
                .endObject().endObject();
    }

    private static XContentBuilder source(String id, int number) throws IOException {
        return jsonBuilder().startObject().startObject("doc")
                .field("id", id)
                .field("numeric1", number)
                .field("numeric2", number)
                .field("numeric3", number)
                .field("numeric4", number)
                .field("numeric5", number)
                .field("numeric6", number)
                .field("numeric7", number)
                .field("numeric8", number)
                .field("numeric9", number)
                .field("numeric10", number)
                .endObject().endObject();
    }
}
