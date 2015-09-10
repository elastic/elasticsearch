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
package org.elasticsearch.benchmark.search.child;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.children.Children;

import java.util.Arrays;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ChildSearchBenchmark {

    /*
        Run: MAVEN_OPTS=-Xmx4g mvn test-compile exec:java -Dexec.mainClass="org.elasticsearch.benchmark.search.child.ChildSearchBenchmark" -Dexec.classpathScope="test" -Dexec.args="bwc false"
     */

    public static void main(String[] args) throws Exception {
        boolean bwcMode = false;
        int numParents = (int) SizeValue.parseSizeValue("2m").singles();;

        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Uneven number of arguments");
        }
        for (int i = 0; i < args.length; i += 2) {
            String value = args[i + 1];
            if ("--bwc_mode".equals(args[i])) {
                bwcMode = Boolean.valueOf(value);
            } else if ("--num_parents".equals(args[i])) {
                numParents = Integer.valueOf(value);
            }
        }


        Settings.Builder settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0);

        // enable bwc parent child mode:
        if (bwcMode) {
            settings.put("tests.mock.version", Version.V_1_6_0);
        }

        String clusterName = ChildSearchBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings.build()).put("name", "node1")).node();
        Client client = node1.client();

        int CHILD_COUNT = 15;
        int QUERY_VALUE_RATIO = 3;
        int QUERY_WARMUP = 10;
        int QUERY_COUNT = 20;
        String indexName = "test";

        ParentChildIndexGenerator parentChildIndexGenerator = new ParentChildIndexGenerator(client, numParents, CHILD_COUNT, QUERY_VALUE_RATIO);
        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        try {
            client.admin().indices().create(createIndexRequest(indexName)).actionGet();
            client.admin().indices().preparePutMapping(indexName).setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("child")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).execute().actionGet();
            Thread.sleep(5000);
            long startTime = System.currentTimeMillis();
            parentChildIndexGenerator.index();
            System.out.println("--> Indexing took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds.");
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount(indexName).setQuery(matchAllQuery()).execute().actionGet().getCount());

        System.out.println("--> Running just child query");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            client.prepareSearch(indexName).setQuery(termQuery("child.tag", "tag1")).execute().actionGet();
        }

        long totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(termQuery("child.tag", "tag1")).execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Just Child Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).execute().actionGet();
        System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());
        
        // run parent child constant query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            boolQuery()
                            .must(matchAllQuery())
                            .filter(hasChildQuery("child", termQuery("field2", parentChildIndexGenerator.getQueryValue())))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            boolQuery()
                            .must(matchAllQuery())
                            .filter(hasChildQuery("child", termQuery("field2", parentChildIndexGenerator.getQueryValue())))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running has_child filter with match_all child query");
        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            boolQuery()
                            .must(matchAllQuery())
                            .filter(hasChildQuery("child", matchAllQuery()))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child filter with match_all child query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");


        System.out.println("--> Running children agg");
        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(matchQuery("field1", parentChildIndexGenerator.getQueryValue()))
                    .addAggregation(
                            AggregationBuilders.children("to-child").childType("child")
                    )
                    .execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            Children children = searchResponse.getAggregations().get("to-child");
            if (j % 10 == 0) {
                System.out.println("--> children doc count [" + j + "], got [" + children.getDocCount() + "]");
            }
        }
        System.out.println("--> children agg, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running children agg with match_all");
        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .addAggregation(
                            AggregationBuilders.children("to-child").childType("child")
                    )
                    .execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            Children children = searchResponse.getAggregations().get("to-child");
            if (j % 10 == 0) {
                System.out.println("--> children doc count [" + j + "], got [" + children.getDocCount() + "]");
            }
        }
        System.out.println("--> children agg, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        // run parent child constant query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            boolQuery()
                            .must(matchAllQuery())
                            .filter(hasParentQuery("parent", termQuery("field1", parentChildIndexGenerator.getQueryValue())))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
        }

        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            boolQuery()
                            .must(matchAllQuery())
                            .filter(hasParentQuery("parent", termQuery("field1", parentChildIndexGenerator.getQueryValue())))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running has_parent filter with match_all parent query ");
        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            boolQuery()
                            .must(matchAllQuery())
                            .filter(hasParentQuery("parent", matchAllQuery()))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent filter with match_all parent query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Field data size: " + statsResponse.getNodes()[0].getIndices().getFieldData().getMemorySize());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        System.out.println("--> Running has_child query with score type");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            client.prepareSearch(indexName).setQuery(hasChildQuery("child", termQuery("field2", parentChildIndexGenerator.getQueryValue())).scoreMode("max")).execute().actionGet();
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", termQuery("field2", parentChildIndexGenerator.getQueryValue())).scoreMode("max")).execute().actionGet();
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", matchAllQuery()).scoreMode("max")).execute().actionGet();
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child query with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        
        System.out.println("--> Running has_parent query with score type");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            client.prepareSearch(indexName).setQuery(hasParentQuery("parent", termQuery("field1", parentChildIndexGenerator.getQueryValue())).scoreMode("score")).execute().actionGet();
        }

        totalQueryTime = 0;
        for (int j = 1; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasParentQuery("parent", termQuery("field1", parentChildIndexGenerator.getQueryValue())).scoreMode("score")).execute().actionGet();
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 1; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasParentQuery("parent", matchAllQuery()).scoreMode("score")).execute().actionGet();
            if (j % 10 == 0) {
                System.out.println("--> hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent query with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.gc();
        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Field data size: " + statsResponse.getNodes()[0].getIndices().getFieldData().getMemorySize());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        client.close();
        node1.close();
    }
}
