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

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.benchmark.search.aggregations.TermsAggregationSearchBenchmark.StatsResult;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.transport.TransportModule;

import java.util.*;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class GlobalOrdinalsBenchmark {

    private static final String INDEX_NAME = "index";
    private static final String TYPE_NAME = "type";
    private static final int QUERY_WARMUP = 25;
    private static final int QUERY_COUNT = 100;
    private static final int FIELD_START = 1;
    private static final int FIELD_LIMIT = 1 << 22;
    private static final boolean USE_DOC_VALUES = false;

    static long COUNT = SizeValue.parseSizeValue("5m").singles();
    static Node node;
    static Client client;

    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.prefix", "");
        BootstrapForTesting.ensureInitialized();
        Random random = new Random();

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(TransportModule.TRANSPORT_TYPE_KEY, "local")
                .build();

        String clusterName = GlobalOrdinalsBenchmark.class.getSimpleName();
        node = nodeBuilder().clusterName(clusterName)
                    .settings(settingsBuilder().put(settings))
                    .node();

        client = node.client();

        try {
            client.admin().indices().prepareCreate(INDEX_NAME)
                    .addMapping(TYPE_NAME, jsonBuilder().startObject().startObject(TYPE_NAME)
                            .startArray("dynamic_templates")
                                .startObject()
                                    .startObject("default")
                                        .field("match", "*")
                                        .field("match_mapping_type", "string")
                                        .startObject("mapping")
                                            .field("type", "string")
                                            .field("index", "not_analyzed")
                                            .startObject("fields")
                                                .startObject("doc_values")
                                                    .field("type", "string")
                                                    .field("index", "no")
                                                    .startObject("fielddata")
                                                        .field("format", "doc_values")
                                                    .endObject()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endArray()
                    .endObject().endObject())
                    .get();
            ObjectHashSet<String> uniqueTerms = new ObjectHashSet<>();
            for (int i = 0; i < FIELD_LIMIT; i++) {
                boolean added;
                do {
                    added = uniqueTerms.add(RandomStrings.randomAsciiOfLength(random, 16));
                } while (!added);
            }
            String[] sValues = uniqueTerms.toArray(String.class);
            uniqueTerms = null;

            BulkRequestBuilder builder = client.prepareBulk();
            IntIntHashMap tracker = new IntIntHashMap();
            for (int i = 0; i < COUNT; i++) {
                Map<String, Object> fieldValues = new HashMap<>();
                for (int fieldSuffix = 1; fieldSuffix <= FIELD_LIMIT; fieldSuffix <<= 1) {
                    int index = tracker.putOrAdd(fieldSuffix, 0, 0);
                    if (index >= fieldSuffix) {
                        index = random.nextInt(fieldSuffix);
                        fieldValues.put("field_" + fieldSuffix, sValues[index]);
                    } else {
                        fieldValues.put("field_" + fieldSuffix, sValues[index]);
                        tracker.put(fieldSuffix, ++index);
                    }
                }
                builder.add(
                        client.prepareIndex(INDEX_NAME, TYPE_NAME, String.valueOf(i))
                        .setSource(fieldValues)
                );

                if (builder.numberOfActions() >= 1000) {
                    builder.get();
                    builder = client.prepareBulk();
                }
            }
            if (builder.numberOfActions() > 0) {
                builder.get();
            }
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }

        client.admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("logger.index.fielddata.ordinals", "DEBUG"))
                .get();

        client.admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet();
        COUNT = client.prepareCount(INDEX_NAME).setQuery(matchAllQuery()).execute().actionGet().getCount();
        System.out.println("--> Number of docs in index: " + COUNT);

        List<StatsResult> stats = new ArrayList<>();
        for (int fieldSuffix = FIELD_START; fieldSuffix <= FIELD_LIMIT; fieldSuffix <<= 1) {
            String fieldName = "field_" + fieldSuffix;
            String name = "global_ordinals-" + fieldName;
            if (USE_DOC_VALUES) {
                fieldName = fieldName + ".doc_values";
                name = name + "_doc_values"; // can't have . in agg name
            }
            stats.add(terms(name, fieldName, "global_ordinals_low_cardinality"));
        }

        for (int fieldSuffix = FIELD_START; fieldSuffix <= FIELD_LIMIT; fieldSuffix <<= 1) {
            String fieldName = "field_" + fieldSuffix;
            String name = "ordinals-" + fieldName;
            if (USE_DOC_VALUES) {
                fieldName = fieldName + ".doc_values";
                name = name + "_doc_values"; // can't have . in agg name
            }
            stats.add(terms(name, fieldName, "ordinals"));
        }

        System.out.println("------------------ SUMMARY -----------------------------------------");
        System.out.format(Locale.ENGLISH, "%30s%10s%10s%15s\n", "name", "took", "millis", "fieldata size");
        for (StatsResult stat : stats) {
            System.out.format(Locale.ENGLISH, "%30s%10s%10d%15s\n", stat.name, TimeValue.timeValueMillis(stat.took), (stat.took / QUERY_COUNT), stat.fieldDataMemoryUsed);
        }
        System.out.println("------------------ SUMMARY -----------------------------------------");

        client.close();
        node.close();
    }

    private static StatsResult terms(String name, String field, String executionHint) {
        long totalQueryTime;// LM VALUE

        client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();
        System.gc();

        System.out.println("--> Warmup (" + name + ")...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(INDEX_NAME)
                    .setSize(0)
                    .setQuery(matchAllQuery())
                    .addAggregation(AggregationBuilders.terms(name).field(field).executionHint(executionHint))
                    .get();
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
            SearchResponse searchResponse = client.prepareSearch(INDEX_NAME)
                    .setSize(0)
                    .setQuery(matchAllQuery())
                    .addAggregation(AggregationBuilders.terms(name).field(field).executionHint(executionHint))
                    .get();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Terms Agg (" + name + "): " + (totalQueryTime / QUERY_COUNT) + "ms");

        String nodeId = node.injector().getInstance(Discovery.class).localNode().getId();
        ClusterStatsResponse clusterStateResponse = client.admin().cluster().prepareClusterStats().setNodesIds(nodeId).get();
        System.out.println("--> Heap used: " + clusterStateResponse.getNodesStats().getJvm().getHeapUsed());
        ByteSizeValue fieldDataMemoryUsed = clusterStateResponse.getIndicesStats().getFieldData().getMemorySize();
        System.out.println("--> Fielddata memory size: " + fieldDataMemoryUsed);

        return new StatsResult(name, totalQueryTime, fieldDataMemoryUsed);
    }

}
