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

package org.elasticsearch.benchmark.search;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.SuggestBuilders;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class CompletionSearchBenchMark {

    public static void main(String[] args) throws Exception {
        int SEARCH_ITERS = 200;

        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("path.home", Files.createTempDirectory("completion-bench"))
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }


        Client client = nodes[0].client();
        try {
            client.admin().indices().prepareDelete("test").get();
        } catch (Exception e) {
            // index does not exist
        }
        // create an index with completion field
        client.admin().indices().prepareCreate("test").setSettings(settings).addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("enabled", false).endObject()
                .startObject("properties")
                .startObject("name_suggest")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject().endObject()).execute().actionGet();

        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            System.err.println("--> Timed out waiting for cluster health");
        }

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        final Path path = Paths.get("/Users/areek/workspace/allCountries.txt");
        BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset());
        int count = 0;
        int totalDocs = 0;
        int batchSize = 100;
        String line = null;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("indexing");
        Set<String> prefixes = new HashSet<>();
        while ((line = reader.readLine()) != null) {
            final String[] fields = line.split("\\t");
            assert fields.length == 19;
            String locationName = fields[1];
            if (locationName.length() > 4) {
                prefixes.add(locationName.substring(0, 3));
            }
            double lat = Double.parseDouble(fields[4]);
            double lon = Double.parseDouble(fields[5]);
            String countryCode = fields[8];
            long population = Long.parseLong(fields[16]);
            //System.out.println("name: " + locationName + " (lat, lon): (" + lat + ", " + lon + ") countery: " + countryCode + " population: " + population);
            count++;

            bulkRequest.add(Requests.indexRequest("test").type("type1").source("name_suggest", locationName));
            if (batchSize == count) {
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    System.err.println("failures.." + bulkResponse.buildFailureMessage());
                }
                bulkRequest = client.prepareBulk();
                totalDocs += count;

                if (totalDocs == 1000000) {
                    break;
                }
                count = 0;
            }
        }
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

        /*
        try {
            client.admin().indices().prepareDelete("test").get();
            client.admin().indices().prepareCreate("test").setSettings(settings).addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("properties")
                    .startObject("name_suggest")
                        .field("type", "completion")
                    .endObject()
                    .endObject()
                    .endObject().endObject()).execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }


            StopWatch stopWatch = new StopWatch().start();
            long COUNT = SizeValue.parseSizeValue("10m").singles();
            int BATCH = 100;
            System.out.println("Indexing [" + COUNT + "] ...");
            long ITERS = COUNT / BATCH;
            long i = 1;
            char character = 'a';
            int idCounter = 0;
            for (; i <= ITERS; i++) {
                int termCounter = 0;
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(idCounter++)).source(source("prefix" + character + termCounter++)));
                }
                character++;
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("failures...");
                }
            }
            System.out.println("Indexing took " + stopWatch.totalTime());

            client.admin().indices().prepareRefresh().execute().actionGet();
            System.out.println("Count: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
            client.admin().indices().prepareRefresh().execute().actionGet();
            System.out.println("Count: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());
        }


        System.out.println("Warming up...");
        char startChar = 'a';
        for (int i = 0; i <= 20; i++) {
            String term = "prefix" + startChar;
            SearchResponse response = client.prepareSearch()
                    .setQuery(prefixQuery("field", term))
                    .addSuggestion(SuggestBuilders.termSuggestion("field").field("field").text(term).suggestMode("always"))
                    .execute().actionGet();
            if (response.getHits().totalHits() == 0) {
                System.err.println("No hits");
                continue;
            }
            startChar++;
        }


        System.out.println("Starting benchmarking suggestions.");
        startChar = 'a';
        long timeTaken = 0;
        for (int i = 0; i <= SEARCH_ITERS; i++) {
            String term = "prefix" + startChar;
            SearchResponse response = client.prepareSearch()
                    .setQuery(matchQuery("field", term))
                    .addSuggestion(SuggestBuilders.termSuggestion("field").text(term).field("field").suggestMode("always"))
                    .execute().actionGet();
            timeTaken += response.getTookInMillis();
            if (response.getSuggest() == null) {
                System.err.println("No suggestions");
                continue;
            }
            List<? extends Option> options = response.getSuggest().getSuggestion("field").getEntries().get(0).getOptions();
            if (options == null || options.isEmpty()) {
                System.err.println("No suggestions");
            }
            startChar++;
        }

        System.out.println("Avg time taken without filter " + (timeTaken / SEARCH_ITERS));
        */
        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String nameValue) throws IOException {
        return jsonBuilder().startObject()
                .field("field", nameValue)
                .endObject();
    }

}
