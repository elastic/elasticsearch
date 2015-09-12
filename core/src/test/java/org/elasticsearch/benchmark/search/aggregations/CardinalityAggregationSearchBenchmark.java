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

package org.elasticsearch.benchmark.search.aggregations;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;

public class CardinalityAggregationSearchBenchmark {

    private static final Random R = new Random();
    private static final String CLUSTER_NAME = CardinalityAggregationSearchBenchmark.class.getSimpleName();
    private static final int NUM_DOCS = 10000000;
    private static final int LOW_CARD = 1000;
    private static final int HIGH_CARD = 1000000;
    private static final int BATCH = 100;
    private static final int WARM = 5;
    private static final int RUNS = 10;
    private static final int ITERS = 5;

    public static void main(String[] args) {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 5)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().clusterName(CLUSTER_NAME)
                    .settings(settingsBuilder().put(settings).put("name", "node" + i))
                    .node();
        }

        Node clientNode = nodeBuilder()
                .clusterName(CLUSTER_NAME)
                .settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        Client client = clientNode.client();

        try {
            client.admin().indices().create(createIndexRequest("index").settings(settings).mapping("type",
                    jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("low_card_str_value")
                            .field("type", "multi_field")
                            .startObject("fields")
                                .startObject("low_card_str_value")
                                    .field("type", "string")
                                .endObject()
                                .startObject("hash")
                                    .field("type", "murmur3")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("high_card_str_value")
                            .field("type", "multi_field")
                            .startObject("fields")
                                .startObject("high_card_str_value")
                                    .field("type", "string")
                                .endObject()
                                .startObject("hash")
                                    .field("type", "murmur3")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("low_card_num_value")
                            .field("type", "long")
                        .endObject()
                        .startObject("high_card_num_value")
                            .field("type", "long")
                        .endObject()
                    .endObject().endObject().endObject())).actionGet();

            System.out.println("Indexing " + NUM_DOCS + " documents");

            StopWatch stopWatch = new StopWatch().start();
            for (int i = 0; i < NUM_DOCS; ) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH && i < NUM_DOCS; ++j) {
                    final int lowCard = RandomInts.randomInt(R, LOW_CARD);
                    final int highCard = RandomInts.randomInt(R, HIGH_CARD);
                    request.add(client.prepareIndex("index", "type", Integer.toString(i)).setSource("low_card_str_value", "str" + lowCard, "high_card_str_value", "str" + highCard, "low_card_num_value", lowCard , "high_card_num_value", highCard));
                    ++i;
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                    System.err.println(response.buildFailureMessage());
                }
                if ((i % 100000) == 0) {
                    System.out.println("--> Indexed " + i + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }

            client.admin().indices().prepareRefresh("index").execute().actionGet();
        } catch (Exception e) {
            System.out.println("Index already exists, skipping index creation");
        }

        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            System.err.println("--> Timed out waiting for cluster health");
        }

        for (int i = 0; i < WARM + RUNS; ++i) {
            if (i >= WARM) {
                System.out.println("RUN " + (i - WARM));
            }
            for (String field : new String[] {"low_card_str_value", "low_card_str_value.hash", "high_card_str_value", "high_card_str_value.hash", "low_card_num_value", "high_card_num_value"}) {
                long start = System.nanoTime();
                SearchResponse resp = null;
                for (int j = 0; j < ITERS; ++j) {
                    resp = client.prepareSearch("index").setSize(0).addAggregation(cardinality("cardinality").field(field)).execute().actionGet();
                }
                long end = System.nanoTime();
                final long cardinality = ((Cardinality) resp.getAggregations().get("cardinality")).getValue();
                if (i >= WARM) {
                    System.out.println(field + "\t" + new TimeValue((end - start) / ITERS, TimeUnit.NANOSECONDS) + "\tcardinality=" + cardinality);
                }
            }
        }
    }

}
