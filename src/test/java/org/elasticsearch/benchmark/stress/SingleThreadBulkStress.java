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

package org.elasticsearch.benchmark.stress;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Random;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class SingleThreadBulkStress {

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        int shardsCount = Integer.parseInt(System.getProperty("es.shards", "1"));
        int replicaCount = Integer.parseInt(System.getProperty("es.replica", "1"));
        boolean autoGenerateId = true;

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "1s")
                .put("index.merge.async", true)
                .put("index.translog.flush_threshold_ops", 5000)
                .put(SETTING_NUMBER_OF_SHARDS, shardsCount)
                .put(SETTING_NUMBER_OF_REPLICAS, replicaCount)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }

        //Node client = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();
        Node client = nodes[0];

        Client client1 = client.client();

        Thread.sleep(1000);
        client1.admin().indices().prepareCreate("test").setSettings(settings).addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_source").field("enabled", false).endObject()
                .startObject("_all").field("enabled", false).endObject()
                .startObject("_type").field("index", "no").endObject()
                .startObject("_id").field("index", "no").endObject()
                .startObject("properties")
                .startObject("field").field("type", "string").field("index", "not_analyzed").field("omit_norms", true).endObject()
//                .startObject("field").field("index", "analyzed").field("omit_norms", false).endObject()
                .endObject()
                .endObject().endObject()).execute().actionGet();
        Thread.sleep(5000);

        StopWatch stopWatch = new StopWatch().start();
        long COUNT = SizeValue.parseSizeValue("2m").singles();
        int BATCH = 500;
        System.out.println("Indexing [" + COUNT + "] ...");
        long ITERS = COUNT / BATCH;
        long i = 1;
        int counter = 0;
        for (; i <= ITERS; i++) {
            BulkRequestBuilder request = client1.prepareBulk();
            for (int j = 0; j < BATCH; j++) {
                counter++;
                request.add(Requests.indexRequest("test").type("type1").id(autoGenerateId ? null : Integer.toString(counter)).source(source(Integer.toString(counter), "test" + counter)));
            }
            BulkResponse response = request.execute().actionGet();
            if (response.hasFailures()) {
                System.err.println("failures...");
            }
            if (((i * BATCH) % 10000) == 0) {
                System.out.println("Indexed " + (i * BATCH) + " took " + stopWatch.stop().lastTaskTime());
                stopWatch.start();
            }
        }
        System.out.println("Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) COUNT) / stopWatch.totalTime().secondsFrac()));

        client.client().admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("Count: " + client.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());

        client.close();

        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("field", nameValue).endObject();
    }
}
