/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
public class SingleThreadBulkStress {

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        Settings settings = settingsBuilder()
                .put("cluster.routing.schedule", 200, TimeUnit.MILLISECONDS)
                .put("index.engine.robin.refreshInterval", "-1")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, 2)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Node node1 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "server1")).node();
        Node node2 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "server2")).node();

        Node client = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        Client client1 = client.client();

        Thread.sleep(1000);
        client1.admin().indices().create(createIndexRequest("test")).actionGet();
        Thread.sleep(5000);

        StopWatch stopWatch = new StopWatch().start();
        long COUNT = SizeValue.parseSizeValue("5m").singles();
        int BATCH = 100;
        System.out.println("Indexing [" + COUNT + "] ...");
        long ITERS = COUNT / BATCH;
        long i = 1;
        int counter = 0;
        for (; i <= ITERS; i++) {
            BulkRequestBuilder request = client1.prepareBulk();
            for (int j = 0; j < BATCH; j++) {
                counter++;
                request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(counter)).source(source(Integer.toString(counter), "test" + counter)));
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
        System.out.println("Count: " + client.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().count());

        client.close();

        node1.close();
        node2.close();
    }

    private static XContentBuilder source(String id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
