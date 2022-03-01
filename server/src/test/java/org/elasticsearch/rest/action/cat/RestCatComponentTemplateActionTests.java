/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestCatComponentTemplateActionTests extends RestActionTestCase {

    private static RestCatComponentTemplateAction action;
    private ClusterName clusterName;
    private ClusterState clusterState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestCatComponentTemplateAction();
        clusterName = new ClusterName("cluster-1");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(new DiscoveryNode("node-1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        ComponentTemplate ct1 = new ComponentTemplate(
            new Template(Settings.builder().put("number_of_replicas", 2).put("index.blocks.write", true).build(), null, null),
            2L,
            null
        );
        Map<String, ComponentTemplate> componentTemplateMap = new HashMap<>();
        componentTemplateMap.put("test_ct", ct1);
        Metadata metadata = Metadata.builder().componentTemplates(componentTemplateMap).build();
        clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterName = null;
        clusterState = null;
    }

    public void testRestCatComponentAction() throws Exception {
        FakeRestRequest getCatComponentTemplateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("_cat/component_templates")
            .build();
        FakeRestChannel channel = new FakeRestChannel(getCatComponentTemplateRequest, true, 0);

        // execute action
        try (NoOpNodeClient nodeClient = buildNodeClient()) {
            action.handleRequest(getCatComponentTemplateRequest, channel, nodeClient);
        }

        // validate results
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertThat(channel.capturedResponse().content().utf8ToString(), equalTo("test_ct 2 0 0 2 0 []\n"));
    }

    public void testRestCatComponentActionWithParam() throws Exception {
        FakeRestRequest getCatComponentTemplateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("_cat/component_templates")
            .withParams(singletonMap("name", "test"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(getCatComponentTemplateRequest, true, 0);

        // execute action
        try (NoOpNodeClient nodeClient = buildNodeClient()) {
            action.handleRequest(getCatComponentTemplateRequest, channel, nodeClient);
        }

        // validate results
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertThat(channel.capturedResponse().content().utf8ToString(), emptyString());
    }

    private NoOpNodeClient buildNodeClient() {
        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, clusterState, false);

        return new NoOpNodeClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) clusterStateResponse);
            }
        };
    }
}
