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

package org.elasticsearch.stresstest.refresh;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.util.UUID;

/**
 */
public class RefreshStressTest1 {

    public static void main(String[] args) throws InterruptedException, IOException {
        int numberOfShards = 5;
        Node node = NodeBuilder.nodeBuilder().local(true).loadConfigSettings(false).clusterName("testCluster").settings(
                ImmutableSettings.settingsBuilder()
                        .put("node.name", "node1")
                        .put("index.number_of_shards", numberOfShards)
                                //.put("path.data", new File("target/data").getAbsolutePath())
                        .build()).node();
        Node node2 = NodeBuilder.nodeBuilder().local(true).loadConfigSettings(false).clusterName("testCluster").settings(
                ImmutableSettings.settingsBuilder()
                        .put("node.name", "node2")
                        .put("index.number_of_shards", numberOfShards)
                                //.put("path.data", new File("target/data").getAbsolutePath())
                        .build()).node();
        Client client = node.client();

        for (int loop = 1; loop < 1000; loop++) {
            String indexName = "testindex" + loop;
            String typeName = "testType" + loop;
            String id = UUID.randomUUID().toString();
            String mapping = "{ \"" + typeName + "\" :  {\"dynamic_templates\" : [{\"no_analyze_strings\" : {\"match_mapping_type\" : \"string\",\"match\" : \"*\",\"mapping\" : {\"type\" : \"string\",\"index\" : \"not_analyzed\"}}}]}}";
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(mapping).execute().actionGet();
//      sleep after put mapping
//      Thread.sleep(100);

            System.out.println("indexing " + loop);
            String name = "name" + id;
            client.prepareIndex(indexName, typeName, id).setSource("{ \"id\": \"" + id + "\", \"name\": \"" + name + "\" }").execute().actionGet();

            client.admin().indices().prepareRefresh(indexName).execute().actionGet();
//      sleep after refresh
//      Thread.sleep(100);

            System.out.println("searching " + loop);
            SearchResponse result = client.prepareSearch(indexName).setPostFilter(FilterBuilders.termFilter("name", name)).execute().actionGet();
            if (result.getHits().hits().length != 1) {
                for (int i = 1; i <= 100; i++) {
                    System.out.println("retry " + loop + ", " + i + ", previous total hits: " + result.getHits().getTotalHits());
                    client.admin().indices().prepareRefresh(indexName).execute().actionGet();
                    Thread.sleep(100);
                    result = client.prepareSearch(indexName).setPostFilter(FilterBuilders.termFilter("name", name)).execute().actionGet();
                    if (result.getHits().hits().length == 1) {
                        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
                        result = client.prepareSearch(indexName).setPostFilter(FilterBuilders.termFilter("name", name)).execute().actionGet();
                        throw new RuntimeException("Record found after " + (i * 100) + " ms, second go: " + result.getHits().hits().length);
                    } else if (i == 100) {
                        if (client.prepareGet(indexName, typeName, id).execute().actionGet().isExists())
                            throw new RuntimeException("Record wasn't found after 10s but can be get by id");
                        else throw new RuntimeException("Record wasn't found after 10s and can't be get by id");
                    }
                }
            }

            //client.admin().indices().prepareDelete(indexName).execute().actionGet();
        }
        client.close();
        node2.close();
        node.close();
    }
}
