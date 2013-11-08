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

package org.elasticsearch.search.basic;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class TransportSearchFailuresTests extends ElasticsearchIntegrationTest {

    @Test
    public void testFailedSearchWithWrongQuery() throws Exception {
        logger.info("Start Testing failed search with wrong query");
        prepareCreate("test", 1, settingsBuilder().put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 2)
                    .put("routing.hash.type", "simple")).execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 100; i++) {
            index(client(), Integer.toString(i), "test", i);
        }
        RefreshResponse refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(9));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(3));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(searchRequest("test").source("{ xxx }".getBytes(Charsets.UTF_8))).actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(3));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(3));
                assert false : "search should fail";
            } catch (ElasticSearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        allowNodes("test", 2);
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes(">=2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest("test")
                .waitForYellowStatus().waitForRelocatingShards(0).waitForActiveShards(6)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getActiveShards(), equalTo(6));

        refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(9));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(6));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(searchRequest("test").source("{ xxx }".getBytes(Charsets.UTF_8))).actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(3));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(3));
                assert false : "search should fail";
            } catch (ElasticSearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        logger.info("Done Testing failed search");
    }

    private void index(Client client, String id, String nameValue, int age) throws IOException {
        client.index(Requests.indexRequest("test").type("type1").id(id).source(source(id, nameValue, age)).consistencyLevel(WriteConsistencyLevel.ONE)).actionGet();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
                .field("id", id)
                .field("name", nameValue + id)
                .field("age", age)
                .field("multi", multi.toString())
                .field("_boost", age * 10)
                .endObject();
    }
}
