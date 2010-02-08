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

package org.elasticsearch.test.integration.nodesinfo;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleNodesInfoTests extends AbstractServersTests {

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testNodesInfos() {
        startServer("server1");
        startServer("server2");
        String server1NodeId = ((InternalServer) server("server1")).injector().getInstance(ClusterService.class).state().nodes().localNodeId();
        String server2NodeId = ((InternalServer) server("server2")).injector().getInstance(ClusterService.class).state().nodes().localNodeId();

        NodesInfoResponse response = client("server1").admin().cluster().nodesInfo(nodesInfo()).actionGet();
        assertThat(response.nodes().length, equalTo(2));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfo()).actionGet();
        assertThat(response.nodes().length, equalTo(2));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());

        response = client("server1").admin().cluster().nodesInfo(nodesInfo(server1NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfo(server1NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());

        response = client("server1").admin().cluster().nodesInfo(nodesInfo(server2NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfo(server2NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());
    }
}
