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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;

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
public class SingleThreadIndexingStress {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "1s")
                .put("index.merge.async", true)
                .put("index.translog.flush_threshold_ops", 5000)
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
        int ID_RANGE = 100;
        System.out.println("Indexing [" + COUNT + "] ...");
        int i = 1;
        for (; i <= COUNT; i++) {
//            client1.admin().cluster().preparePingSingle("test", "type1", Integer.toString(i)).execute().actionGet();
            client1.prepareIndex("test", "type1").setId(Integer.toString(i % ID_RANGE)).setSource(source(Integer.toString(i), "test" + i))
                    .setCreate(false).execute().actionGet();
            if ((i % 10000) == 0) {
                System.out.println("Indexed " + i + " took " + stopWatch.stop().lastTaskTime());
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
