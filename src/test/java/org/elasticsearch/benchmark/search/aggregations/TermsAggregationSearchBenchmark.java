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

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.collect.Lists;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.jna.Natives;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class TermsAggregationSearchBenchmark {

    static long COUNT = SizeValue.parseSizeValue("2m").singles();
    static int BATCH = 1000;
    static int QUERY_WARMUP = 10;
    static int QUERY_COUNT = 100;
    static int NUMBER_OF_TERMS = 200;
    static int NUMBER_OF_MULTI_VALUE_TERMS = 10;
    static int STRING_TERM_SIZE = 5;

    static Client client;
    static Node[] nodes;

    public enum Method {
        AGGREGATION {
            @Override
            SearchRequestBuilder addTermsAgg(SearchRequestBuilder builder, String name, String field, String executionHint) {
                return builder.addAggregation(AggregationBuilders.terms(name).executionHint(executionHint).field(field));
            }

            @Override
            SearchRequestBuilder addTermsStatsAgg(SearchRequestBuilder builder, String name, String keyField, String valueField) {
                return builder.addAggregation(AggregationBuilders.terms(name).field(keyField).subAggregation(AggregationBuilders.stats("stats").field(valueField)));
            }
        },
        AGGREGATION_DEFERRED {
            @Override
            SearchRequestBuilder addTermsAgg(SearchRequestBuilder builder, String name, String field, String executionHint) {
                return builder.addAggregation(AggregationBuilders.terms(name).executionHint(executionHint).field(field).collectMode(SubAggCollectionMode.BREADTH_FIRST));
            }

            @Override
            SearchRequestBuilder addTermsStatsAgg(SearchRequestBuilder builder, String name, String keyField, String valueField) {
                return builder.addAggregation(AggregationBuilders.terms(name).field(keyField).collectMode(SubAggCollectionMode.BREADTH_FIRST).subAggregation(AggregationBuilders.stats("stats").field(valueField)));
            }
        };
        abstract SearchRequestBuilder addTermsAgg(SearchRequestBuilder builder, String name, String field, String executionHint);
        abstract SearchRequestBuilder addTermsStatsAgg(SearchRequestBuilder builder, String name, String keyField, String valueField);
    }

    public static void main(String[] args) throws Exception {
        Natives.tryMlockall();
        Random random = new Random();

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = TermsAggregationSearchBenchmark.class.getSimpleName();
        nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().clusterName(clusterName)
                    .settings(settingsBuilder().put(settings).put("name", "node" + i))
                    .node();
        }

        Node clientNode = nodeBuilder()
                .clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        client = clientNode.client();

        Thread.sleep(10000);
        try {
            client.admin().indices().create(createIndexRequest("test").mapping("type1", jsonBuilder()
              .startObject()
                .startObject("type1")
                  .startObject("properties")
                    .startObject("s_value_dv")
                      .field("type", "string")
                      .field("index", "no")
                      .startObject("fielddata")
                        .field("format", "doc_values")
                      .endObject()
                    .endObject()
                    .startObject("sm_value_dv")
                      .field("type", "string")
                      .field("index", "no")
                      .startObject("fielddata")
                        .field("format", "doc_values")
                      .endObject()
                    .endObject()
                    .startObject("l_value_dv")
                      .field("type", "long")
                      .field("index", "no")
                      .startObject("fielddata")
                        .field("format", "doc_values")
                      .endObject()
                    .endObject()
                    .startObject("lm_value_dv")
                      .field("type", "long")
                      .field("index", "no")
                      .startObject("fielddata")
                        .field("format", "doc_values")
                      .endObject()
                    .endObject()
                  .endObject()
                .endObject()
              .endObject())).actionGet();

            ObjectOpenHashSet<String> uniqueTerms = ObjectOpenHashSet.newInstance();
            for (int i = 0; i < NUMBER_OF_TERMS; i++) {
                boolean added;
                do {
                    added = uniqueTerms.add(RandomStrings.randomAsciiOfLength(random, STRING_TERM_SIZE));
                } while (!added);
            }
            String[] sValues = uniqueTerms.toArray(String.class);
            uniqueTerms = null;

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] ...");
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;

                    XContentBuilder builder = jsonBuilder().startObject();
                    builder.field("id", Integer.toString(counter));
                    final String sValue = sValues[ThreadLocalRandom.current().nextInt(sValues.length)];
                    final long lValue = ThreadLocalRandom.current().nextInt(NUMBER_OF_TERMS);
                    builder.field("s_value", sValue);
                    builder.field("l_value", lValue);
                    builder.field("s_value_dv", sValue);
                    builder.field("l_value_dv", lValue);

                    for (String field : new String[] {"sm_value", "sm_value_dv"}) {
                        builder.startArray(field);
                        for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
                            builder.value(sValues[ThreadLocalRandom.current().nextInt(sValues.length)]);
                        }
                        builder.endArray();
                    }

                    for (String field : new String[] {"lm_value", "lm_value_dv"}) {
                        builder.startArray(field);
                        for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
                            builder.value(ThreadLocalRandom.current().nextInt(NUMBER_OF_TERMS));
                        }
                        builder.endArray();
                    }

                    builder.endObject();

                    request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(counter))
                            .source(builder));
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * BATCH) + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }
            System.out.println("--> Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) (COUNT)) / stopWatch.totalTime().secondsFrac()));
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        COUNT = client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
        System.out.println("--> Number of docs in index: " + COUNT);


        List<StatsResult> stats = Lists.newArrayList();
        stats.add(terms("terms_agg_s", Method.AGGREGATION, "s_value", null));
        stats.add(terms("terms_agg_s_dv", Method.AGGREGATION, "s_value_dv", null));
        stats.add(terms("terms_agg_map_s", Method.AGGREGATION, "s_value", "map"));
        stats.add(terms("terms_agg_map_s_dv", Method.AGGREGATION, "s_value_dv", "map"));
        stats.add(terms("terms_agg_def_s", Method.AGGREGATION_DEFERRED, "s_value", null));
        stats.add(terms("terms_agg_def_s_dv", Method.AGGREGATION_DEFERRED, "s_value_dv", null));
        stats.add(terms("terms_agg_def_map_s", Method.AGGREGATION_DEFERRED, "s_value", "map"));
        stats.add(terms("terms_agg_def_map_s_dv", Method.AGGREGATION_DEFERRED, "s_value_dv", "map"));
        stats.add(terms("terms_agg_l", Method.AGGREGATION, "l_value", null));
        stats.add(terms("terms_agg_l_dv", Method.AGGREGATION, "l_value_dv", null));
        stats.add(terms("terms_agg_def_l", Method.AGGREGATION_DEFERRED, "l_value", null));
        stats.add(terms("terms_agg_def_l_dv", Method.AGGREGATION_DEFERRED, "l_value_dv", null));
        stats.add(terms("terms_agg_sm", Method.AGGREGATION, "sm_value", null));
        stats.add(terms("terms_agg_sm_dv", Method.AGGREGATION, "sm_value_dv", null));
        stats.add(terms("terms_agg_map_sm", Method.AGGREGATION, "sm_value", "map"));
        stats.add(terms("terms_agg_map_sm_dv", Method.AGGREGATION, "sm_value_dv", "map"));
        stats.add(terms("terms_agg_def_sm", Method.AGGREGATION_DEFERRED, "sm_value", null));
        stats.add(terms("terms_agg_def_sm_dv", Method.AGGREGATION_DEFERRED, "sm_value_dv", null));
        stats.add(terms("terms_agg_def_map_sm", Method.AGGREGATION_DEFERRED, "sm_value", "map"));
        stats.add(terms("terms_agg_def_map_sm_dv", Method.AGGREGATION_DEFERRED, "sm_value_dv", "map"));
        stats.add(terms("terms_agg_lm", Method.AGGREGATION, "lm_value", null));
        stats.add(terms("terms_agg_lm_dv", Method.AGGREGATION, "lm_value_dv", null));
        stats.add(terms("terms_agg_def_lm", Method.AGGREGATION_DEFERRED, "lm_value", null));
        stats.add(terms("terms_agg_def_lm_dv", Method.AGGREGATION_DEFERRED, "lm_value_dv", null));

        stats.add(termsStats("terms_stats_agg_s_l", Method.AGGREGATION, "s_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_s_l_dv", Method.AGGREGATION, "s_value_dv", "l_value_dv", null));
        stats.add(termsStats("terms_stats_agg_def_s_l", Method.AGGREGATION_DEFERRED, "s_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_def_s_l_dv", Method.AGGREGATION_DEFERRED, "s_value_dv", "l_value_dv", null));
        stats.add(termsStats("terms_stats_agg_s_lm", Method.AGGREGATION, "s_value", "lm_value", null));
        stats.add(termsStats("terms_stats_agg_s_lm_dv", Method.AGGREGATION, "s_value_dv", "lm_value_dv", null));
        stats.add(termsStats("terms_stats_agg_def_s_lm", Method.AGGREGATION_DEFERRED, "s_value", "lm_value", null));
        stats.add(termsStats("terms_stats_agg_def_s_lm_dv", Method.AGGREGATION_DEFERRED, "s_value_dv", "lm_value_dv", null));
        stats.add(termsStats("terms_stats_agg_sm_l", Method.AGGREGATION, "sm_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_sm_l_dv", Method.AGGREGATION, "sm_value_dv", "l_value_dv", null));
        stats.add(termsStats("terms_stats_agg_def_sm_l", Method.AGGREGATION_DEFERRED, "sm_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_def_sm_l_dv", Method.AGGREGATION_DEFERRED, "sm_value_dv", "l_value_dv", null));

        stats.add(termsStats("terms_stats_agg_s_l", Method.AGGREGATION, "s_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_s_l_dv", Method.AGGREGATION, "s_value_dv", "l_value_dv", null));
        stats.add(termsStats("terms_stats_agg_def_s_l", Method.AGGREGATION_DEFERRED, "s_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_def_s_l_dv", Method.AGGREGATION_DEFERRED, "s_value_dv", "l_value_dv", null));
        stats.add(termsStats("terms_stats_agg_s_lm", Method.AGGREGATION, "s_value", "lm_value", null));
        stats.add(termsStats("terms_stats_agg_s_lm_dv", Method.AGGREGATION, "s_value_dv", "lm_value_dv", null));
        stats.add(termsStats("terms_stats_agg_def_s_lm", Method.AGGREGATION_DEFERRED, "s_value", "lm_value", null));
        stats.add(termsStats("terms_stats_agg_def_s_lm_dv", Method.AGGREGATION_DEFERRED, "s_value_dv", "lm_value_dv", null));
        stats.add(termsStats("terms_stats_agg_sm_l", Method.AGGREGATION, "sm_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_sm_l_dv", Method.AGGREGATION, "sm_value_dv", "l_value_dv", null));
        stats.add(termsStats("terms_stats_agg_def_sm_l", Method.AGGREGATION_DEFERRED, "sm_value", "l_value", null));
        stats.add(termsStats("terms_stats_agg_def_sm_l_dv", Method.AGGREGATION_DEFERRED, "sm_value_dv", "l_value_dv", null));

        System.out.println("------------------ SUMMARY ----------------------------------------------");
        System.out.format(Locale.ENGLISH, "%35s%10s%10s%15s\n", "name", "took", "millis", "fieldata size");
        for (StatsResult stat : stats) {
            System.out.format(Locale.ENGLISH, "%35s%10s%10d%15s\n", stat.name, TimeValue.timeValueMillis(stat.took), (stat.took / QUERY_COUNT), stat.fieldDataMemoryUsed);
        }
        System.out.println("------------------ SUMMARY ----------------------------------------------");

        clientNode.close();

        for (Node node : nodes) {
            node.close();
        }
    }

    public static class StatsResult {
        final String name;
        final long took;
        final ByteSizeValue fieldDataMemoryUsed;

        public StatsResult(String name, long took, ByteSizeValue fieldDataMemoryUsed) {
            this.name = name;
            this.took = took;
            this.fieldDataMemoryUsed = fieldDataMemoryUsed;
        }
    }

    private static StatsResult terms(String name, Method method, String field, String executionHint) {
        long totalQueryTime;// LM VALUE

        client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();
        System.gc();

        System.out.println("--> Warmup (" + name + ")...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = method.addTermsAgg(client.prepareSearch("test")
                    .setSize(0)
                    .setQuery(matchAllQuery()), name, field, executionHint)
                    .execute().actionGet();
            if (j == 0) {
                System.out.println("--> Loading (" + field + "): took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }
        System.out.println("--> Warmup (" + name + ") DONE");


        System.out.println("--> Running (" + name + ")...");
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = method.addTermsAgg(client.prepareSearch()
                    .setSize(0)
                    .setQuery(matchAllQuery()), name, field, executionHint)
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Terms Agg (" + name + "): " + (totalQueryTime / QUERY_COUNT) + "ms");

        String[] nodeIds = new String[nodes.length];
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = nodes[i].injector().getInstance(Discovery.class).localNode().getId();
        }

        ClusterStatsResponse clusterStateResponse = client.admin().cluster().prepareClusterStats().setNodesIds(nodeIds).get();
        System.out.println("--> Heap used: " + clusterStateResponse.getNodesStats().getJvm().getHeapUsed());
        ByteSizeValue fieldDataMemoryUsed = clusterStateResponse.getIndicesStats().getFieldData().getMemorySize();
        System.out.println("--> Fielddata memory size: " + fieldDataMemoryUsed);

        return new StatsResult(name, totalQueryTime, fieldDataMemoryUsed);
    }

    private static StatsResult termsStats(String name, Method method, String keyField, String valueField, String executionHint) {
        long totalQueryTime;

        client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();
        System.gc();

        System.out.println("--> Warmup (" + name + ")...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = method.addTermsStatsAgg(client.prepareSearch()
                    .setSize(0)
                    .setQuery(matchAllQuery()), name, keyField, valueField)
                    .execute().actionGet();
            if (j == 0) {
                System.out.println("--> Loading (" + name + "): took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }
        System.out.println("--> Warmup (" + name + ") DONE");


        System.out.println("--> Running (" + name + ")...");
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = method.addTermsStatsAgg(client.prepareSearch()
                    .setSize(0)
                    .setQuery(matchAllQuery()), name, keyField, valueField)
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Terms stats agg (" + name + "): " + (totalQueryTime / QUERY_COUNT) + "ms");
        return new StatsResult(name, totalQueryTime, ByteSizeValue.parseBytesSizeValue("0b"));
    }
}
