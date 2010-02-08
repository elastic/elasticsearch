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
import org.elasticsearch.server.Server;
import org.elasticsearch.util.StopWatch;
import org.elasticsearch.util.settings.Settings;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.server.ServerBuilder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleMemoryMonitorBenchmark {

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        Settings settings = settingsBuilder()
                .putTime("cluster.routing.schedule", 200, TimeUnit.MILLISECONDS)
                .putInt(SETTING_NUMBER_OF_SHARDS, 5)
                .putInt(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Server server1 = serverBuilder().settings(settingsBuilder().putAll(settings).put("name", "server1")).server();
        Server server2 = serverBuilder().settings(settingsBuilder().putAll(settings).put("name", "server2")).server();

        Client client1 = server1.client();

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

        server1.close();
        server2.close();
    }

    private static String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
