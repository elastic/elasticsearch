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

package org.elasticsearch.plugin.ingest.transport.reload;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

public class ReloadPipelinesActionTests extends ESTestCase {

    private ClusterService clusterService;
    private TransportService transportService;
    private ReloadPipelinesAction reloadPipelinesAction;

    @Before
    public void init() {
        Settings settings = Settings.EMPTY;
        PipelineStore pipelineStore = mock(PipelineStore.class);
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        reloadPipelinesAction = new ReloadPipelinesAction(settings, pipelineStore, clusterService, transportService);
    }

    public void testSuccess() {
        int numNodes = randomIntBetween(1, 10);
        int numIngestNodes = 0;

        DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            boolean ingestNode = i == 0 || randomBoolean();
            DiscoveryNode discoNode = generateDiscoNode(i, ingestNode);
            discoNodes.put(discoNode);
            if (ingestNode) {
                numIngestNodes++;
            }
        }
        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(discoNodes).build();
        when(clusterService.state()).thenReturn(state);

        final int finalNumIngestNodes = numIngestNodes;
        doAnswer(mock -> {
            TransportResponseHandler handler = (TransportResponseHandler) mock.getArguments()[3];
            for (int i = 0; i < finalNumIngestNodes; i++) {
                handler.handleResponse(new ReloadPipelinesAction.ReloadPipelinesResponse());
            }
            return mock;
        }).when(transportService).sendRequest(any(), eq(ReloadPipelinesAction.ACTION_NAME), any(), any());
        reloadPipelinesAction.reloadPipelinesOnAllNodes(result -> assertThat(result, is(true)));
    }

    public void testWithAtLeastOneFailure() {
        int numNodes = randomIntBetween(1, 10);
        int numIngestNodes = 0;

        DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            boolean ingestNode = i == 0 || randomBoolean();
            DiscoveryNode discoNode = generateDiscoNode(i, ingestNode);
            discoNodes.put(discoNode);
            if (ingestNode) {
                numIngestNodes++;
            }
        }
        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(discoNodes).build();
        when(clusterService.state()).thenReturn(state);

        final int finalNumIngestNodes = numIngestNodes;
        doAnswer(mock -> {
            TransportResponseHandler handler = (TransportResponseHandler) mock.getArguments()[3];
            handler.handleException(new TransportException("test failure"));
            for (int i = 1; i < finalNumIngestNodes; i++) {
                if (randomBoolean()) {
                    handler.handleResponse(new ReloadPipelinesAction.ReloadPipelinesResponse());
                } else {
                    handler.handleException(new TransportException("test failure"));
                }
            }
            return mock;
        }).when(transportService).sendRequest(any(), eq(ReloadPipelinesAction.ACTION_NAME), any(), any());
        reloadPipelinesAction.reloadPipelinesOnAllNodes(result -> assertThat(result, is(false)));
    }

    public void testNoIngestNodes() {
        // expected exception if there are no nodes:
        DiscoveryNodes discoNodes = DiscoveryNodes.builder()
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(discoNodes).build();
        when(clusterService.state()).thenReturn(state);

        try {
            reloadPipelinesAction.reloadPipelinesOnAllNodes(result -> fail("shouldn't be invoked"));
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("There are no ingest nodes in this cluster"));
        }

        // expected exception if there are no ingest nodes:
        discoNodes = DiscoveryNodes.builder()
            .put(new DiscoveryNode("_name", "_id", new LocalTransportAddress("_id"), Collections.singletonMap("ingest", "false"), Version.CURRENT))
            .build();
        state = ClusterState.builder(new ClusterName("_name")).nodes(discoNodes).build();
        when(clusterService.state()).thenReturn(state);

        try {
            reloadPipelinesAction.reloadPipelinesOnAllNodes(result -> fail("shouldn't be invoked"));
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("There are no ingest nodes in this cluster"));
        }
    }

    private DiscoveryNode generateDiscoNode(int index, boolean ingestNode) {
        Map<String, String> attributes;
        if (ingestNode) {
            attributes = Collections.singletonMap("ingest", "true");
        } else {
            attributes = Collections.emptyMap();
        }
        String id = String.valueOf(index);
        return new DiscoveryNode(id, id, new LocalTransportAddress(id), attributes, Version.CURRENT);
    }

}
