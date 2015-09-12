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

import com.carrotsearch.hppc.ObjectScatterSet;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.search.aggregations.TermsAggregationSearchBenchmark.Method;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class TermsAggregationSearchAndIndexingBenchmark {

    static String indexName = "test";
    static String typeName = "type1";
    static Random random = new Random();

    static long COUNT = SizeValue.parseSizeValue("2m").singles();
    static int BATCH = 1000;
    static int NUMBER_OF_TERMS = (int) SizeValue.parseSizeValue("100k").singles();
    static int NUMBER_OF_MULTI_VALUE_TERMS = 10;
    static int STRING_TERM_SIZE = 5;

    static Node[] nodes;

    public static void main(String[] args) throws Exception {
        BootstrapForTesting.ensureInitialized();
        Settings settings = settingsBuilder()
                .put("refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = TermsAggregationSearchAndIndexingBenchmark.class.getSimpleName();
        nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node1"))
                    .clusterName(clusterName)
                    .node();
        }
        Client client = nodes[0].client();

        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        try {
            client.admin().indices().prepareCreate(indexName)
                    .addMapping(typeName, generateMapping("eager", "lazy"))
                    .get();
            Thread.sleep(5000);

            long startTime = System.currentTimeMillis();
            ObjectScatterSet<String> uniqueTerms = new ObjectScatterSet<>();
            for (int i = 0; i < NUMBER_OF_TERMS; i++) {
                boolean added;
                do {
                    added = uniqueTerms.add(RandomStrings.randomAsciiOfLength(random, STRING_TERM_SIZE));
                } while (!added);
            }
            String[] sValues = uniqueTerms.toArray(String.class);
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;

                    XContentBuilder builder = jsonBuilder().startObject();
                    builder.field("id", Integer.toString(counter));
                    final String sValue = sValues[counter % sValues.length];
                    builder.field("s_value", sValue);
                    builder.field("s_value_dv", sValue);

                    for (String field : new String[] {"sm_value", "sm_value_dv"}) {
                        builder.startArray(field);
                        for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
                            builder.value(sValues[ThreadLocalRandom.current().nextInt(sValues.length)]);
                        }
                        builder.endArray();
                    }

                    request.add(Requests.indexRequest(indexName).type("type1").id(Integer.toString(counter))
                            .source(builder));
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * BATCH));
                }
            }

            System.out.println("--> Indexing took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds.");
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().preparePutMapping(indexName)
                .setType(typeName)
                .setSource(generateMapping("lazy", "lazy"))
                .get();
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());


        String[] nodeIds = new String[nodes.length];
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = nodes[i].injector().getInstance(Discovery.class).localNode().getId();
        }

        List<TestRun> testRuns = new ArrayList<>();
        testRuns.add(new TestRun("Regular field ordinals", "eager", "lazy", "s_value", "ordinals"));
        testRuns.add(new TestRun("Docvalues field ordinals", "lazy", "eager", "s_value_dv", "ordinals"));
        testRuns.add(new TestRun("Regular field global ordinals", "eager_global_ordinals", "lazy", "s_value", null));
        testRuns.add(new TestRun("Docvalues field global", "lazy", "eager_global_ordinals", "s_value_dv", null));

        List<TestResult> testResults = new ArrayList<>();
        for (TestRun testRun : testRuns) {
            client.admin().indices().preparePutMapping(indexName).setType(typeName)
                    .setSource(generateMapping(testRun.indexedFieldEagerLoading, testRun.docValuesEagerLoading)).get();
            client.admin().indices().prepareClearCache(indexName).setFieldDataCache(true).get();
            SearchThread searchThread = new SearchThread(client, testRun.termsAggsField, testRun.termsAggsExecutionHint);
            RefreshThread refreshThread = new RefreshThread(client);
            System.out.println("--> Running '" + testRun.name + "' round...");
            new Thread(refreshThread).start();
            new Thread(searchThread).start();
            Thread.sleep(2 * 60 * 1000);
            refreshThread.stop();
            searchThread.stop();

            System.out.println("--> Avg refresh time: " + refreshThread.avgRefreshTime + " ms");
            System.out.println("--> Avg query time: " + searchThread.avgQueryTime + " ms");

            ClusterStatsResponse clusterStateResponse = client.admin().cluster().prepareClusterStats().setNodesIds(nodeIds).get();
            System.out.println("--> Heap used: " + clusterStateResponse.getNodesStats().getJvm().getHeapUsed());
            ByteSizeValue fieldDataMemoryUsed = clusterStateResponse.getIndicesStats().getFieldData().getMemorySize();
            System.out.println("--> Fielddata memory size: " + fieldDataMemoryUsed);
            testResults.add(new TestResult(testRun.name, refreshThread.avgRefreshTime, searchThread.avgQueryTime, fieldDataMemoryUsed));
        }

        System.out.println("----------------------------------------- SUMMARY ----------------------------------------------");
        System.out.format(Locale.ENGLISH, "%30s%18s%15s%15s\n", "name", "avg refresh time", "avg query time", "fieldata size");
        for (TestResult testResult : testResults) {
            System.out.format(Locale.ENGLISH, "%30s%18s%15s%15s\n", testResult.name, testResult.avgRefreshTime, testResult.avgQueryTime, testResult.fieldDataSizeInMemory);
        }
        System.out.println("----------------------------------------- SUMMARY ----------------------------------------------");

        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    static class RefreshThread implements Runnable {

        private final Client client;
        private volatile boolean run = true;
        private volatile boolean stopped = false;
        private volatile long avgRefreshTime = 0;

        RefreshThread(Client client) throws IOException {
            this.client = client;
        }

        @Override
        public void run() {
            long totalRefreshTime = 0;
            int numExecutedRefreshed = 0;
            while (run) {
                long docIdLimit = COUNT;
                for (long docId = 1; run && docId < docIdLimit;) {
                    try {
                        for (int j = 0; j < 8; j++) {
                            GetResponse getResponse = client
                                    .prepareGet(indexName, "type1", String.valueOf(++docId))
                                    .get();
                            client.prepareIndex(indexName, "type1", getResponse.getId())
                                    .setSource(getResponse.getSource())
                                    .get();
                        }
                        long startTime = System.currentTimeMillis();
                        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
                        totalRefreshTime += System.currentTimeMillis() - startTime;
                        numExecutedRefreshed++;
                        Thread.sleep(500);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
            avgRefreshTime = totalRefreshTime / numExecutedRefreshed;
            stopped = true;
        }

        public void stop() throws InterruptedException {
            run = false;
            while (!stopped) {
                Thread.sleep(100);
            }
        }

    }

    private static class TestRun {

        final String name;
        final String indexedFieldEagerLoading;
        final String docValuesEagerLoading;
        final String termsAggsField;
        final String termsAggsExecutionHint;

        private TestRun(String name, String indexedFieldEagerLoading, String docValuesEagerLoading, String termsAggsField, String termsAggsExecutionHint) {
            this.name = name;
            this.indexedFieldEagerLoading = indexedFieldEagerLoading;
            this.docValuesEagerLoading = docValuesEagerLoading;
            this.termsAggsField = termsAggsField;
            this.termsAggsExecutionHint = termsAggsExecutionHint;
        }
    }

    private static class TestResult {

        final String name;
        final TimeValue avgRefreshTime;
        final TimeValue avgQueryTime;
        final ByteSizeValue fieldDataSizeInMemory;

        private TestResult(String name, long avgRefreshTime, long avgQueryTime, ByteSizeValue fieldDataSizeInMemory) {
            this.name = name;
            this.avgRefreshTime = TimeValue.timeValueMillis(avgRefreshTime);
            this.avgQueryTime = TimeValue.timeValueMillis(avgQueryTime);
            this.fieldDataSizeInMemory = fieldDataSizeInMemory;
        }
    }

    static class SearchThread implements Runnable {

        private final Client client;
        private final String field;
        private final String executionHint;
        private volatile boolean run = true;
        private volatile boolean stopped = false;
        private volatile long avgQueryTime = 0;

        SearchThread(Client client, String field, String executionHint) {
            this.client = client;
            this.field = field;
            this.executionHint = executionHint;
        }

        @Override
        public void run() {
            long totalQueryTime = 0;
            int numExecutedQueries = 0;
            while (run) {
                try {
                    SearchResponse searchResponse = Method.AGGREGATION.addTermsAgg(client.prepareSearch()
                            .setSize(0)
                            .setQuery(matchAllQuery()), "test", field, executionHint)
                            .execute().actionGet();
                    if (searchResponse.getHits().totalHits() != COUNT) {
                        System.err.println("--> mismatch on hits");
                    }
                    totalQueryTime += searchResponse.getTookInMillis();
                    numExecutedQueries++;
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            avgQueryTime = totalQueryTime / numExecutedQueries;
            stopped = true;
        }

        public void stop() throws InterruptedException {
            run = false;
            while (!stopped) {
                Thread.sleep(100);
            }
        }

    }

    private static XContentBuilder generateMapping(String loading1, String loading2) throws IOException {
        return jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("s_value")
                .field("type", "string")
                .field("index", "not_analyzed")
                .startObject("fielddata")
                .field("loading", loading1)
                .endObject()
                .endObject()
                .startObject("s_value_dv")
                .field("type", "string")
                .field("index", "no")
                .startObject("fielddata")
                .field("loading", loading2)
                .field("format", "doc_values")
                .endObject()
                .endObject()
                .endObject().endObject().endObject();
    }

}
