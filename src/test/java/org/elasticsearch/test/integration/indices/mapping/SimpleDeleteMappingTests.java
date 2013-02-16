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

package org.elasticsearch.test.integration.indices.mapping;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleDeleteMappingTests extends AbstractNodesTests {

    protected Client client1;
    protected Client client2;

    @BeforeMethod
    public void startNodes() {
        startNode("node1");
        startNode("node2");
        client1 = getClient1();
        client2 = getClient2();

        createIndex();
    }

    protected void createIndex() {
        logger.info("Creating index test");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();
    }

    @AfterMethod
    public void closeNodes() {
        client1.close();
        client2.close();
        closeAllNodes();
    }

    protected Client getClient1() {
        return client("node1");
    }

    protected Client getClient2() {
        return client("node2");
    }

    @Test
    public void simpleDeleteMapping() throws Exception {
        for (int i = 0; i < 10; i++) {
            client1.prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).execute().actionGet();
        }

        ClusterHealthResponse clusterHealth = client1.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        client1.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client1.prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(countResponse.count(), equalTo(10l));
        }

        ClusterState clusterState = client1.admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.metaData().index("test").mappings().containsKey("type1"), equalTo(true));

        client1.admin().indices().prepareDeleteMapping().setType("type1").execute().actionGet();
        Thread.sleep(500); // for now, we don't have ack logic, so just wait

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client1.prepareCount().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(countResponse.count(), equalTo(0l));
        }

        clusterState = client1.admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.metaData().index("test").mappings().containsKey("type1"), equalTo(false));
    }
}
