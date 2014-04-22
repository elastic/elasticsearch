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

import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
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
public class TimeDataHistogramAggregationBenchmark {

    static long COUNT = SizeValue.parseSizeValue("5m").singles();
    static long TIME_PERIOD = 24 * 3600 * 1000;
    static int BATCH = 100;
    static int QUERY_WARMUP = 50;
    static int QUERY_COUNT = 500;
    static IndexFieldData.CommonSettings.MemoryStorageFormat MEMORY_FORMAT = IndexFieldData.CommonSettings.MemoryStorageFormat.PAGED;
    static double ACCEPTABLE_OVERHEAD_RATIO = 0.5;
    static float MATCH_PERCENTAGE = 0.1f;

    static Client client;

    public static void main(String[] args) throws Exception {

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put("gateway.type", "local")
                .put("node.local", true)
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = TimeDataHistogramAggregationBenchmark.class.getSimpleName();
        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().clusterName(clusterName)
                    .settings(settingsBuilder().put(settings).put("name", "node" + i))
                    .node();
        }

        client = nodes[0].client();

        Thread.sleep(10000);
        try {
            client.admin().indices().create(createIndexRequest("test")).actionGet();

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] ...");
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            long[] currentTimeInMillis1 = new long[]{System.currentTimeMillis()};
            long[] currentTimeInMillis2 = new long[]{System.currentTimeMillis()};
            long startTimeInMillis = currentTimeInMillis1[0];
            long averageMillisChange = TIME_PERIOD / COUNT * 2;
            long backwardSkew = Math.max(1, (long) (averageMillisChange * 0.1));
            long bigOutOfOrder = 1;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;

                    XContentBuilder builder = jsonBuilder().startObject();
                    builder.field("id", Integer.toString(counter));
                    // move forward in time and sometimes a little bit back (delayed delivery)
                    long diff = ThreadLocalRandom.current().nextLong(2 * averageMillisChange + 2 * backwardSkew) - backwardSkew;
                    long[] currentTime = counter % 2 == 0 ? currentTimeInMillis1 : currentTimeInMillis2;
                    currentTime[0] += diff;
                    if (ThreadLocalRandom.current().nextLong(100) <= bigOutOfOrder) {
                        builder.field("l_value", currentTime[0] - 60000); // 1m delays
                    } else {
                        builder.field("l_value", currentTime[0]);
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
            System.out.println("Time range 1: " + (currentTimeInMillis1[0] - startTimeInMillis) / 1000.0 / 3600 + " hours");
            System.out.println("Time range 2: " + (currentTimeInMillis2[0] - startTimeInMillis) / 1000.0 / 3600 + " hours");
            System.out.println("--> optimizing index");
            client.admin().indices().prepareOptimize().setMaxNumSegments(1).get();
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        COUNT = client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
        System.out.println("--> Number of docs in index: " + COUNT);

        // load with the reverse options to make sure jit doesn't optimize one away
        setMapping(ACCEPTABLE_OVERHEAD_RATIO, MEMORY_FORMAT.equals(IndexFieldData.CommonSettings.MemoryStorageFormat.PACKED) ? IndexFieldData.CommonSettings.MemoryStorageFormat.PAGED : IndexFieldData.CommonSettings.MemoryStorageFormat.PACKED);
        warmUp("hist_l", "l_value", MATCH_PERCENTAGE);

        setMapping(ACCEPTABLE_OVERHEAD_RATIO, MEMORY_FORMAT);
        warmUp("hist_l", "l_value", MATCH_PERCENTAGE);

        List<StatsResult> stats = Lists.newArrayList();
        stats.add(measureAgg("hist_l", "l_value", MATCH_PERCENTAGE));

        NodesStatsResponse nodeStats = client.admin().cluster().prepareNodesStats(nodes[0].settings().get("name")).clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.FieldData)).get();


        System.out.println("------------------ SUMMARY -------------------------------");

        System.out.println("docs: " + COUNT);
        System.out.println("match percentage: " + MATCH_PERCENTAGE);
        System.out.println("memory format hint: " + MEMORY_FORMAT);
        System.out.println("acceptable_overhead_ratio: " + ACCEPTABLE_OVERHEAD_RATIO);
        System.out.println("field data: " + nodeStats.getNodes()[0].getIndices().getFieldData().getMemorySize());
        System.out.format(Locale.ROOT, "%25s%10s%10s\n", "name", "took", "millis");
        for (StatsResult stat : stats) {
            System.out.format(Locale.ROOT, "%25s%10s%10d\n", stat.name, TimeValue.timeValueMillis(stat.took), (stat.took / QUERY_COUNT));
        }
        System.out.println("------------------ SUMMARY -------------------------------");

        for (Node node : nodes) {
            node.close();
        }
    }

    protected static void setMapping(double acceptableOverheadRatio, IndexFieldData.CommonSettings.MemoryStorageFormat fielddataStorageFormat) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject().startObject("type1").startObject("properties").startObject("l_value")
                .field("type", "long")
                .startObject("fielddata")
                .field("acceptable_transient_overhead_ratio", acceptableOverheadRatio)
                .field("acceptable_overhead_ratio", acceptableOverheadRatio)
                .field(IndexFieldData.CommonSettings.SETTING_MEMORY_STORAGE_HINT, fielddataStorageFormat.name().toLowerCase(Locale.ROOT))
                .endObject()
                .endObject().endObject().endObject().endObject();
        client.admin().indices().preparePutMapping("test").setType("type1").setSource(mapping).get();
    }

    static class StatsResult {
        final String name;
        final long took;

        StatsResult(String name, long took) {
            this.name = name;
            this.took = took;
        }
    }

    private static SearchResponse doTermsAggsSearch(String name, String field, float matchPercentage) {
        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("random()<matchP").addParam("matchP", matchPercentage).cache(true)))
                .addAggregation(AggregationBuilders.histogram(name).field(field).interval(3600 * 1000)).get();

        if (response.getHits().totalHits() < COUNT * matchPercentage * 0.7) {
            System.err.println("--> warning - big deviation from expected count: " + response.getHits().totalHits() + " expected: " + COUNT * matchPercentage);
        }

        return response;
    }

    private static StatsResult measureAgg(String name, String field, float matchPercentage) {
        long totalQueryTime;// LM VALUE

        System.out.println("--> Running (" + name + ")...");
        totalQueryTime = 0;
        long previousCount = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = doTermsAggsSearch(name, field, matchPercentage);
            if (previousCount == 0) {
                previousCount = searchResponse.getHits().getTotalHits();
            } else if (searchResponse.getHits().totalHits() != previousCount) {
                System.err.println("*** HIT COUNT CHANGE -> CACHE EXPIRED? ***");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Histogram aggregations (" + field + "): " + (totalQueryTime / QUERY_COUNT) + "ms");
        return new StatsResult(name, totalQueryTime);
    }

    private static void warmUp(String name, String field, float matchPercentage) {
        System.out.println("--> Warmup (" + name + ")...");
        client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();

        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = doTermsAggsSearch(name, field, matchPercentage);
            if (j == 0) {
                System.out.println("--> Loading (" + field + "): took: " + searchResponse.getTook());
            }
        }
        System.out.println("--> Warmup (" + name + ") DONE");
    }
}
