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
import org.elasticsearch.common.unit.SizeUnit;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class HDRPercentilesAggregationBenchmark {

    private static final String TYPE_NAME = "type";
    private static final String INDEX_NAME = "index";
    private static final String HIGH_CARD_FIELD_NAME = "high_card";
    private static final String LOW_CARD_FIELD_NAME = "low_card";
    private static final String GAUSSIAN_FIELD_NAME = "gauss";
    private static final Random R = new Random();
    private static final String CLUSTER_NAME = HDRPercentilesAggregationBenchmark.class.getSimpleName();
    private static final int NUM_DOCS = 10000000;
    private static final int LOW_CARD = 1000;
    private static final int HIGH_CARD = 1000000;
    private static final int BATCH = 100;
    private static final int WARM = 5;
    private static final int RUNS = 10;
    private static final int ITERS = 5;

    public static void main(String[] args) {
        long overallStartTime = System.currentTimeMillis();
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
            client.admin().indices().prepareCreate(INDEX_NAME);

            System.out.println("Indexing " + NUM_DOCS + " documents");

            StopWatch stopWatch = new StopWatch().start();
            for (int i = 0; i < NUM_DOCS; ) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH && i < NUM_DOCS; ++j) {
                    final int lowCard = RandomInts.randomInt(R, LOW_CARD);
                    final int highCard = RandomInts.randomInt(R, HIGH_CARD);
                    int gauss = -1;
                    while (gauss < 0) {
                        gauss = (int) (R.nextGaussian() * 1000) + 5000; // mean: 5 sec, std deviation: 1 sec
                    }
                    request.add(client.prepareIndex(INDEX_NAME, TYPE_NAME, Integer.toString(i)).setSource(LOW_CARD_FIELD_NAME, lowCard,
                            HIGH_CARD_FIELD_NAME, highCard, GAUSSIAN_FIELD_NAME, gauss));
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

            client.admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet();
        } catch (Exception e) {
            System.out.println("Index already exists, skipping index creation");
        }

        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            System.err.println("--> Timed out waiting for cluster health");
        }

        System.out.println("Run\tField\tMethod\tAggregationTime\tEstimatedMemory");
        for (int i = 0; i < WARM + RUNS; ++i) {
            for (String field : new String[] { LOW_CARD_FIELD_NAME, HIGH_CARD_FIELD_NAME, GAUSSIAN_FIELD_NAME }) {
                for (PercentilesMethod method : new PercentilesMethod[] {PercentilesMethod.TDIGEST, PercentilesMethod.HDR}) {
                    long start = System.nanoTime();
                    SearchResponse resp = null;
                    for (int j = 0; j < ITERS; ++j) {
                        resp = client.prepareSearch(INDEX_NAME).setSize(0).addAggregation(percentiles("percentiles").field(field).method(method)).execute().actionGet();
                    }
                    long end = System.nanoTime();
                    long memoryEstimate = 0;
                    switch (method) {
                    case TDIGEST:
                        memoryEstimate = ((InternalTDigestPercentiles) resp.getAggregations().get("percentiles"))
                                .getEstimatedMemoryFootprint();
                        break;
                    case HDR:
                        memoryEstimate = ((InternalHDRPercentiles) resp.getAggregations().get("percentiles")).getEstimatedMemoryFootprint();
                        break;
                    }
                    if (i >= WARM) {
                        System.out.println((i - WARM) + "\t" + field + "\t" + method + "\t"
                                + new TimeValue((end - start) / ITERS, TimeUnit.NANOSECONDS).millis() + "\t"
                                + new SizeValue(memoryEstimate, SizeUnit.SINGLE).singles());
                    }
                }
            }
        }
        long overallEndTime = System.currentTimeMillis();
        System.out.println("Benchmark completed in " + ((overallEndTime - overallStartTime) / 1000) + " seconds");
    }

}
