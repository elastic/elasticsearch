/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.stress.updatebyquery;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.updatebyquery.IndexUpdateByQueryResponse;
import org.elasticsearch.action.updatebyquery.UpdateByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Map;

/**
 */
public class UpdateByQueryStressTest {

    public static void main(String[] args) {
        final int NUMBER_OF_NODES = 4;
        final int NUMBER_OF_INDICES = 5;
        final int BATCH = 300000;

        final Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 2)
//                .put("action.updatebyquery.bulk_size", 5)
                .put("index.number_of_replicas", 0).build();
        final Node[] nodes = new Node[NUMBER_OF_NODES];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(nodeSettings).node();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                for (Node node : nodes) {
                    node.close();
                }
            }
        }));

        try {
            Client client = nodes.length == 1 ? nodes[0].client() : nodes[1].client();
            try {
                client.admin().indices().prepareDelete().execute().actionGet();
            } catch (Exception e) {
                // ignore
            }

            client.admin().indices().prepareUpdateSettings("*").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1));
            for (int i = 0; i < NUMBER_OF_INDICES; i++) {
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    bulkRequest.add(Requests.indexRequest("test" + i).id(Integer.toString(j)).type("type").source("field", "1"));
                    if (bulkRequest.numberOfActions() % 10000 == 0) {
                        bulkRequest.execute().actionGet();
                        bulkRequest = client.prepareBulk();
                    }
                }
                if (bulkRequest.numberOfActions() > 0) {
                    bulkRequest.execute().actionGet();
                }
            }
            client.admin().indices().prepareRefresh("*").execute().actionGet();
            client.admin().indices().prepareUpdateSettings("*").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", 1));
            client.admin().cluster().prepareHealth("*").setWaitForGreenStatus().execute().actionGet();

            UpdateByQueryResponse response = client.prepareUpdateByQuery()
                    .setIndices("*")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setScript("ctx._source.field += 1")
                    .execute()
                    .actionGet();

            System.out.printf("Update by query took: %d ms and matches with %d documents\n", response.tookInMillis(), response.totalHits());
            System.out.printf("and %d documents have actually successfully been updated.\n", response.updated());
            if (response.totalHits() != BATCH * NUMBER_OF_INDICES) {
                System.err.printf(
                        "Number of matches is incorrect! Expected %d but was %d\n",
                        BATCH * NUMBER_OF_INDICES,
                        response.totalHits()
                );
            }

            if (response.indexResponses().length != NUMBER_OF_INDICES) {
                System.err.printf(
                        "Number of index sub responses is incorrect! Expected %d but was %d\n",
                        BATCH,
                        response.indexResponses().length
                );
            }

            for (IndexUpdateByQueryResponse indexResponse : response.indexResponses()) {
                for (Map.Entry<Integer, BulkItemResponse[]> bulkItemResponses : indexResponse.responsesByShard().entrySet()) {
                    for (BulkItemResponse bulkItemResponse : bulkItemResponses.getValue()) {
                        IndexResponse indexRes = (IndexResponse) bulkItemResponse.response();
                        if (indexRes.version() != 2) {
                            System.out.printf(
                                    "Version doesn't match for id[%s] expected version 2, but was %d\n",
                                    indexRes.id(),
                                    indexRes.version()
                            );
                        }
                    }
                }
            }

        } finally {
            for (Node node : nodes) {
                node.close();
            }
        }

    }
}
