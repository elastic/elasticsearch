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

package org.elasticsearch.test.integration.gateway.fs;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class FsMetaDataGatewayTests extends AbstractNodesTests {

    @AfterMethod
    void closeNodes() throws Exception {
        node("server1").stop();
        // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
        ((InternalNode) node("server1")).injector().getInstance(Gateway.class).reset();
        closeAllNodes();
    }

    @BeforeMethod
    void buildNodeToReset() throws Exception {
        buildNode("server1");
        // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
        ((InternalNode) node("server1")).injector().getInstance(Gateway.class).reset();
        closeAllNodes();
    }

    @Test
    public void testIndexActions() throws Exception {
        startNode("server1");

        logger.info("Running Cluster Health (waiting for node to startup properly)");
        ClusterHealthResponse clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();

        closeNode("server1");

        startNode("server1");
        Thread.sleep(500);
        try {
            client("server1").admin().indices().create(createIndexRequest("test")).actionGet();
            assert false : "index should exists";
        } catch (IndexAlreadyExistsException e) {
            // all is well
        }
    }
}
