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

import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;

import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;

public class PercentilesAggregationSearchBenchmark {

    private static final int AMPLITUDE = 10000;
    private static final int NUM_DOCS = (int) SizeValue.parseSizeValue("1m").singles();
    private static final int BATCH = 100;
    private static final String CLUSTER_NAME = PercentilesAggregationSearchBenchmark.class.getSimpleName();
    private static final double[] PERCENTILES = new double[] { 0, 0.01, 0.1, 1, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100};
    private static final int QUERY_WARMUP = 10;
    private static final int QUERY_COUNT = 20;

    private static Random R = new Random(0);

    // we generate ints to not disadvantage qdigest which only works with integers
    private enum Distribution {
        UNIFORM {
            @Override
            int next() {
                return (int) (R.nextDouble() * AMPLITUDE);
            }
        },
        GAUSS {
            @Override
            int next() {
                return (int) (R.nextDouble() * AMPLITUDE);
            }
        },
        LOG_NORMAL {
            @Override
            int next() {
                return (int) Math.exp(R.nextDouble() * Math.log(AMPLITUDE));
            }
        };
        String indexName() {
            return name().toLowerCase(Locale.ROOT);
        }
        abstract int next();
    }

    private static double accuratePercentile(double percentile, int[] sortedValues) {
        final double index = percentile / 100 * (sortedValues.length - 1);
        final int intIndex = (int) index;
        final double delta = index - intIndex;
        if (delta == 0) {
            return sortedValues[intIndex];
        } else {
            return sortedValues[intIndex] * (1 - delta) + sortedValues[intIndex + 1] * delta;
        }
    }

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 100) // to also test performance and accuracy of the reduce phase
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

        for (Distribution d : Distribution.values()) {
            try {
//                client.admin().indices().prepareDelete(d.indexName()).execute().actionGet();
                client.admin().indices().create(createIndexRequest(d.indexName()).settings(settings)).actionGet();
            } catch (Exception e) {
                System.out.println("Index " + d.indexName() + " already exists, skipping index creation");
                continue;
            }

            final int[] values = new int[NUM_DOCS];
            for (int i = 0; i < NUM_DOCS; ++i) {
                values[i] = d.next();
            }
            System.out.println("Indexing " + NUM_DOCS + " documents into " + d.indexName());
            StopWatch stopWatch = new StopWatch().start();
            for (int i = 0; i < NUM_DOCS; ) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH && i < NUM_DOCS; ++j) {
                    request.add(client.prepareIndex(d.indexName(), "values", Integer.toString(i)).setSource("v", values[i]));
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
            Arrays.sort(values);
            XContentBuilder builder = JsonXContent.contentBuilder().startObject();
            for (double percentile : PERCENTILES) {
                builder.field(Double.toString(percentile), accuratePercentile(percentile, values));
            }
            client.prepareIndex(d.indexName(), "values", "percentiles").setSource(builder.endObject()).execute().actionGet();
            client.admin().indices().prepareRefresh(d.indexName()).execute().actionGet();
        }

        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            System.err.println("--> Timed out waiting for cluster health");
        }

        System.out.println("## Precision");
        for (Distribution d : Distribution.values()) {
            System.out.println("#### " + d);
            final long count = client.prepareCount(d.indexName()).setQuery(matchAllQuery()).execute().actionGet().getCount();
            if (count != NUM_DOCS + 1) {
                throw new Error("Expected " + NUM_DOCS + " documents, got " + (count - 1));
            }
            Map<String, Object> percentilesUnsorted = client.get(getRequest(d.indexName()).type("values").id("percentiles")).actionGet().getSourceAsMap();
            SortedMap<Double, Double> percentiles = Maps.newTreeMap();
            for (Map.Entry<String, Object> entry : percentilesUnsorted.entrySet()) {
                percentiles.put(Double.parseDouble(entry.getKey()), (Double) entry.getValue());
            }
            System.out.println("Expected percentiles: " + percentiles);
            System.out.println();
            SearchResponse resp = client.prepareSearch(d.indexName()).setSearchType(SearchType.COUNT).addAggregation(percentiles("pcts").field("v").percentiles(PERCENTILES)).execute().actionGet();
            Percentiles pcts = resp.getAggregations().get("pcts");
            Map<Double, Double> asMap = Maps.newLinkedHashMap();
            double sumOfErrorSquares = 0;
            for (Percentile percentile : pcts) {
                asMap.put(percentile.getPercent(), percentile.getValue());
                double error = percentile.getValue() - percentiles.get(percentile.getPercent());
                sumOfErrorSquares += error * error;
            }
            System.out.println("Percentiles: " + asMap);
            System.out.println("Sum of error squares: " + sumOfErrorSquares);
            System.out.println();
        }
        
        System.out.println("## Performance");
        for (int i = 0; i < 3; ++i) {
            for (Distribution d : Distribution.values()) {
                System.out.println("#### " + d);
                for (int j = 0; j < QUERY_WARMUP; ++j) {
                    client.prepareSearch(d.indexName()).setSearchType(SearchType.COUNT).addAggregation(percentiles("pcts").field("v").percentiles(PERCENTILES)).execute().actionGet();
                }
                long start = System.nanoTime();
                for (int j = 0; j < QUERY_COUNT; ++j) {
                    client.prepareSearch(d.indexName()).setSearchType(SearchType.COUNT).addAggregation(percentiles("pcts").field("v").percentiles(PERCENTILES)).execute().actionGet();
                }
                System.out.println(new TimeValue((System.nanoTime() - start) / QUERY_COUNT, TimeUnit.NANOSECONDS));
            }
        }
    }

}
