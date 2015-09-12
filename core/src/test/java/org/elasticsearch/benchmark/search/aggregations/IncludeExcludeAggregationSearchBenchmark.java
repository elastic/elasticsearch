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

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class IncludeExcludeAggregationSearchBenchmark {

    private static final Random R = new Random();
    private static final String CLUSTER_NAME = IncludeExcludeAggregationSearchBenchmark.class.getSimpleName();
    private static final int NUM_DOCS = 10000000;
    private static final int BATCH = 100;
    private static final int WARM = 3;
    private static final int RUNS = 10;
    private static final int ITERS = 3;

    public static void main(String[] args) {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
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
                        .startObject("str")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject().endObject().endObject())).actionGet();

            System.out.println("Indexing " + NUM_DOCS + " documents");

            StopWatch stopWatch = new StopWatch().start();
            for (int i = 0; i < NUM_DOCS; ) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH && i < NUM_DOCS; ++j) {
                    request.add(client.prepareIndex("index", "type", Integer.toString(i)).setSource("str", TestUtil.randomSimpleString(R)));
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
            long start = System.nanoTime();
            SearchResponse resp = null;
            for (int j = 0; j < ITERS; ++j) {
                resp = client.prepareSearch("index").setQuery(QueryBuilders.prefixQuery("str", "sf")).setSize(0).addAggregation(terms("t").field("str").include("s.*")).execute().actionGet();
            }
            long end = System.nanoTime();
            if (i >= WARM) {
                System.out.println(new TimeValue((end - start) / ITERS, TimeUnit.NANOSECONDS));
            }
        }
    }

}
