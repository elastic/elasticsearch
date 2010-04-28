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

package org.elasticsearch.benchmark.monitor.memory;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.util.StopWatch;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;
import static org.elasticsearch.util.xcontent.XContentFactory.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleMemoryMonitorBenchmark {

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        Settings settings = settingsBuilder()
                .put("cluster.routing.schedule", 200, TimeUnit.MILLISECONDS)
                .put("index.engine.robin.refreshInterval", 1, TimeUnit.SECONDS)
                .put(SETTING_NUMBER_OF_SHARDS, 5)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Node node1 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "server1")).node();
        Node node2 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "server2")).node();

        Client client1 = node1.client();

        Thread.sleep(1000);
        client1.admin().indices().create(createIndexRequest("test")).actionGet();
        Thread.sleep(5000);

        StopWatch stopWatch = new StopWatch().start();
        int COUNT = 200000;
        System.out.println("Indexing [" + COUNT + "] ...");
        for (int i = 0; i < COUNT; i++) {
            client1.index(
                    indexRequest("test")
                            .type("type1")
                            .id(Integer.toString(i))
                            .source(source(Integer.toString(i), "test" + i))
                            .opType(IndexRequest.OpType.INDEX)
            ).actionGet();
            if ((i % 10000) == 0) {
                System.out.println("Indexed 10000, total " + (i + 10000) + " took " + stopWatch.stop().lastTaskTime());
                stopWatch.start();
            }
        }
        System.out.println("Indexing took " + stopWatch.stop().totalTime());

        node1.close();
        node2.close();
    }

    private static XContentBuilder source(String id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
