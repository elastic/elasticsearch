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

package org.elasticsearch.cluster;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class SimpleDataNodesIT extends ESIntegTestCase {
    public void testDataNodes() throws Exception {
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());
        client().admin().indices().create(createIndexRequest("test").waitForActiveShards(ActiveShardCount.NONE)).actionGet();
        try {
            client().index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"), XContentType.JSON)
                .timeout(timeValueSeconds(1))).actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setLocal(true).execute().actionGet().isTimedOut(), equalTo(false));

        // still no shard should be allocated
        try {
            client().index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"), XContentType.JSON)
                .timeout(timeValueSeconds(1))).actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), true).build());
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").setLocal(true).execute().actionGet().isTimedOut(), equalTo(false));

        IndexResponse indexResponse = client().index(Requests.indexRequest("test").type("type1").id("1")
            .source(source("1", "test"), XContentType.JSON)).actionGet();
        assertThat(indexResponse.getId(), equalTo("1"));
        assertThat(indexResponse.getType(), equalTo("type1"));
    }

    private String source(String id, String nameValue) {
        return "{ \"type1\" : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
