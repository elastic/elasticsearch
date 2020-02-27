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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class RestNodesHotThreadsActionTests extends ESTestCase {

    private RestNodesHotThreadsAction action;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestNodesHotThreadsAction();
    }

    public void testRouteClusterNodesHotthreads() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("nodeId", "node_1");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
        .withPath("/_cluster/nodes/hotthreads").build();
        try {
            action.prepareRequest(request, mock(NodeClient.class));
        } catch (Exception e) {
            
        }
        assertWarnings(RestNodesHotThreadsAction.LOCAL_DEPRECATED_MESSAGE);
    }

    public void testRouteClusterNodesNodeIdHotthreads() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("nodeId", "node_1");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
        .withPath("/_cluster/nodes/{nodeId}/hotthreads").build();
        try {
            action.prepareRequest(request, mock(NodeClient.class));
        } catch (Exception e) {
            
        }
        assertWarnings(RestNodesHotThreadsAction.LOCAL_DEPRECATED_MESSAGE);
    }

    public void testRouteNodesHotthreads() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("nodeId", "node_1");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
        .withPath("/_nodes/hotthreads").build();
        try {
            action.prepareRequest(request, mock(NodeClient.class));
        } catch (Exception e) {
            
        }
        assertWarnings(RestNodesHotThreadsAction.LOCAL_DEPRECATED_MESSAGE);
    }

    public void testRouteNodesNodeIdHotthreads() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("nodeId", "node_1");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
        .withPath("/_nodes/{nodeId}/hotthreads").build();
        try {
            action.prepareRequest(request, mock(NodeClient.class));
        } catch (Exception e) {
            
        }
        assertWarnings(RestNodesHotThreadsAction.LOCAL_DEPRECATED_MESSAGE);
    }

}
