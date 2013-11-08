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

package org.elasticsearch.broadcast;

import com.google.common.base.Charsets;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class BroadcastActionsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBroadcastOperations() throws IOException {
        prepareCreate("test", 1).execute().actionGet(5000);

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        client().index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        flush();
        client().index(indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();
        refresh();

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            CountResponse countResponse = client().count(countRequest("test").query(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.NO_THREADS)).actionGet();
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getTotalShards(), equalTo(5));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }

        for (int i = 0; i < 5; i++) {
            CountResponse countResponse = client().count(countRequest("test").query(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.SINGLE_THREAD)).actionGet();
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getTotalShards(), equalTo(5));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }

        for (int i = 0; i < 5; i++) {
            CountResponse countResponse = client().count(countRequest("test").query(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.THREAD_PER_SHARD)).actionGet();
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getTotalShards(), equalTo(5));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }

        for (int i = 0; i < 5; i++) {
            // test failed (simply query that can't be parsed)
            CountResponse countResponse = client().count(countRequest("test").query("{ term : { _type : \"type1 } }".getBytes(Charsets.UTF_8))).actionGet();

            assertThat(countResponse.getCount(), equalTo(0l));
            assertThat(countResponse.getTotalShards(), equalTo(5));
            assertThat(countResponse.getSuccessfulShards(), equalTo(0));
            assertThat(countResponse.getFailedShards(), equalTo(5));
            for (ShardOperationFailedException exp : countResponse.getShardFailures()) {
                assertThat(exp.reason(), containsString("QueryParsingException"));
            }
        }

    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
