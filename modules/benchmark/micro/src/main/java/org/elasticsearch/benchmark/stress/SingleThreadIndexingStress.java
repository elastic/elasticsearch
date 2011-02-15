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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
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
public class SingleThreadIndexingStress {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("cluster.routing.schedule", 200, TimeUnit.MILLISECONDS)
                .put("index.engine.robin.refreshInterval", "-1")
                .put("gateway.type", "none")
                .put(SETTING_NUMBER_OF_SHARDS, 2)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }

        Node client = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        Client client1 = client.client();

        Thread.sleep(1000);
        client1.admin().indices().create(createIndexRequest("test")).actionGet();
        Thread.sleep(5000);

        StopWatch stopWatch = new StopWatch().start();
        int COUNT = 200000;
        System.out.println("Indexing [" + COUNT + "] ...");
        int i = 1;
        for (; i <= COUNT; i++) {
//            client1.admin().cluster().preparePingSingle("test", "type1", Integer.toString(i)).execute().actionGet();
            client1.prepareIndex("test", "type1").setId(Integer.toString(i)).setSource(source(Integer.toString(i), "test" + i))
                    .setCreate(false).execute().actionGet();
            if ((i % 10000) == 0) {
                System.out.println("Indexed " + i + " took " + stopWatch.stop().lastTaskTime());
                stopWatch.start();
            }
        }
        System.out.println("Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) COUNT) / stopWatch.totalTime().secondsFrac()));

        client.client().admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("Count: " + client.client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().count());

        client.close();

        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String id, String nameValue) throws IOException {
        long time = System.currentTimeMillis();
        return jsonBuilder().startObject()
                .field("id", id)
//                .field("numeric1", time)
//                .field("numeric2", time)
//                .field("numeric3", time)
//                .field("numeric4", time)
//                .field("numeric5", time)
//                .field("numeric6", time)
//                .field("numeric7", time)
//                .field("numeric8", time)
//                .field("numeric9", time)
//                .field("numeric10", time)
                .field("name", nameValue)
                .endObject();
    }
}
