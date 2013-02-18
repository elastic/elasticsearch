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

package org.elasticsearch.test.integration.recovery;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleRecoveryTests extends AbstractNodesTests {

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    protected Settings recoverySettings() {
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

    @Test
    public void testSimpleRecovery() throws Exception {
        startNode("server1", recoverySettings());

        client("server1").admin().indices().create(createIndexRequest("test")).actionGet(5000);

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForYellowStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        client("server1").index(indexRequest("test").setType("type1").setId("1").setSource(source("1", "test"))).actionGet();
        FlushResponse flushResponse = client("server1").admin().indices().flush(flushRequest("test")).actionGet();
        assertThat(flushResponse.getTotalShards(), equalTo(10));
        assertThat(flushResponse.getSuccessfulShards(), equalTo(5));
        assertThat(flushResponse.getFailedShards(), equalTo(0));
        client("server1").index(indexRequest("test").setType("type1").setId("2").setSource(source("2", "test"))).actionGet();
        RefreshResponse refreshResponse = client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(10));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(5));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        startNode("server2", recoverySettings());

        logger.info("Running Cluster Health");
        clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        GetResponse getResult;

        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").setType("type1").setId("1").setOperationThreaded(false)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client("server2").get(getRequest("test").setType("type1").setId("1").setOperationThreaded(false)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").setType("type1").setId("2").setOperationThreaded(true)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
            getResult = client("server2").get(getRequest("test").setType("type1").setId("2").setOperationThreaded(true)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
        }

        // now start another one so we move some primaries
        startNode("server3", recoverySettings());
        Thread.sleep(200);
        logger.info("Running Cluster Health");
        clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").setType("type1").setId("1")).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client("server2").get(getRequest("test").setType("type1").setId("1")).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client("server3").get(getRequest("test").setType("type1").setId("1")).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").setType("type1").setId("2").setOperationThreaded(true)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
            getResult = client("server2").get(getRequest("test").setType("type1").setId("2").setOperationThreaded(true)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
            getResult = client("server3").get(getRequest("test").setType("type1").setId("2").setOperationThreaded(true)).actionGet(1000);
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
        }
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
