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

package org.elasticsearch.benchmark.search.geo;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 */
public class GeoDistanceSearchBenchmark {

    public static void main(String[] args) throws Exception {

        Node node = NodeBuilder.nodeBuilder().clusterName(GeoDistanceSearchBenchmark.class.getSimpleName()).node();
        Client client = node.client();

        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            System.err.println("Failed to wait for green status, bailing");
            System.exit(1);
        }

        final long NUM_DOCS = SizeValue.parseSizeValue("1m").singles();
        final long NUM_WARM = 50;
        final long NUM_RUNS = 100;

        if (client.admin().indices().prepareExists("test").execute().actionGet().isExists()) {
            System.out.println("Found an index, count: " + client.prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount());
        } else {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                    .endObject().endObject().string();
            client.admin().indices().prepareCreate("test")
                    .setSettings(Settings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                    .addMapping("type1", mapping)
                    .execute().actionGet();

            System.err.println("--> Indexing [" + NUM_DOCS + "]");
            for (long i = 0; i < NUM_DOCS; ) {
                client.prepareIndex("test", "type1", Long.toString(i++)).setSource(jsonBuilder().startObject()
                        .field("name", "New York")
                        .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                        .endObject()).execute().actionGet();

                // to NY: 5.286 km
                client.prepareIndex("test", "type1", Long.toString(i++)).setSource(jsonBuilder().startObject()
                        .field("name", "Times Square")
                        .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        .endObject()).execute().actionGet();

                // to NY: 0.4621 km
                client.prepareIndex("test", "type1", Long.toString(i++)).setSource(jsonBuilder().startObject()
                        .field("name", "Tribeca")
                        .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                        .endObject()).execute().actionGet();

                // to NY: 1.258 km
                client.prepareIndex("test", "type1", Long.toString(i++)).setSource(jsonBuilder().startObject()
                        .field("name", "Soho")
                        .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                        .endObject()).execute().actionGet();

                // to NY: 8.572 km
                client.prepareIndex("test", "type1", Long.toString(i++)).setSource(jsonBuilder().startObject()
                        .field("name", "Brooklyn")
                        .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                        .endObject()).execute().actionGet();

                if ((i % 10000) == 0) {
                    System.err.println("--> indexed " + i);
                }
            }
            System.err.println("Done indexed");
            client.admin().indices().prepareFlush("test").execute().actionGet();
            client.admin().indices().prepareRefresh().execute().actionGet();
        }

        System.err.println("--> Warming up (ARC) - optimize_bbox");
        long start = System.currentTimeMillis();
        for (int i = 0; i < NUM_WARM; i++) {
            run(client, GeoDistance.ARC, "memory");
        }
        long totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Warmup (ARC)  - optimize_bbox (memory) " + (totalTime / NUM_WARM) + "ms");

        System.err.println("--> Perf (ARC) - optimize_bbox (memory)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_RUNS; i++) {
            run(client, GeoDistance.ARC, "memory");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Perf (ARC) - optimize_bbox " + (totalTime / NUM_RUNS) + "ms");

        System.err.println("--> Warming up (ARC)  - optimize_bbox (indexed)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_WARM; i++) {
            run(client, GeoDistance.ARC, "indexed");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Warmup (ARC) - optimize_bbox (indexed) " + (totalTime / NUM_WARM) + "ms");

        System.err.println("--> Perf (ARC) - optimize_bbox (indexed)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_RUNS; i++) {
            run(client, GeoDistance.ARC, "indexed");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Perf (ARC) - optimize_bbox (indexed) " + (totalTime / NUM_RUNS) + "ms");


        System.err.println("--> Warming up (ARC)  - no optimize_bbox");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_WARM; i++) {
            run(client, GeoDistance.ARC, "none");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Warmup (ARC) - no optimize_bbox " + (totalTime / NUM_WARM) + "ms");

        System.err.println("--> Perf (ARC) - no optimize_bbox");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_RUNS; i++) {
            run(client, GeoDistance.ARC, "none");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Perf (ARC) - no optimize_bbox " + (totalTime / NUM_RUNS) + "ms");

        System.err.println("--> Warming up (SLOPPY_ARC)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_WARM; i++) {
            run(client, GeoDistance.SLOPPY_ARC, "memory");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Warmup (SLOPPY_ARC) " + (totalTime / NUM_WARM) + "ms");

        System.err.println("--> Perf (SLOPPY_ARC)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_RUNS; i++) {
            run(client, GeoDistance.SLOPPY_ARC, "memory");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Perf (SLOPPY_ARC) " + (totalTime / NUM_RUNS) + "ms");

        System.err.println("--> Warming up (PLANE)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_WARM; i++) {
            run(client, GeoDistance.PLANE, "memory");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Warmup (PLANE) " + (totalTime / NUM_WARM) + "ms");

        System.err.println("--> Perf (PLANE)");
        start = System.currentTimeMillis();
        for (int i = 0; i < NUM_RUNS; i++) {
            run(client, GeoDistance.PLANE, "memory");
        }
        totalTime = System.currentTimeMillis() - start;
        System.err.println("--> Perf (PLANE) " + (totalTime / NUM_RUNS) + "ms");

        node.close();
    }

    public static void run(Client client, GeoDistance geoDistance, String optimizeBbox) {
        client.prepareSearch() // from NY
                .setSize(0)
                .setQuery(boolQuery().must(matchAllQuery()).filter(geoDistanceQuery("location")
                        .distance("2km")
                        .optimizeBbox(optimizeBbox)
                        .geoDistance(geoDistance)
                        .point(40.7143528, -74.0059731)))
                .execute().actionGet();
    }
}
