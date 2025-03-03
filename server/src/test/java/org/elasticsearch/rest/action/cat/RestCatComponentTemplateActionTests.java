/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponseUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
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
        var projectId = randomProjectIdOrDefault();
        action = new RestCatComponentTemplateAction(TestProjectResolvers.singleProject(projectId));
        clusterName = new ClusterName("cluster-1");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(DiscoveryNodeUtils.builder("node-1").roles(emptySet()).build());
        ComponentTemplate ct1 = new ComponentTemplate(
            new Template(Settings.builder().put("number_of_replicas", 2).put("index.blocks.write", true).build(), null, null),
            2L,
            null
        );
        Map<String, ComponentTemplate> componentTemplateMap = new HashMap<>();
        componentTemplateMap.put("test_ct", ct1);
        ProjectMetadata project = ProjectMetadata.builder(projectId).componentTemplates(componentTemplateMap).build();
        clusterState = mock(ClusterState.class);
        final Metadata metadata = Metadata.builder().put(project).build();
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
        try (var threadPool = createThreadPool()) {
            final var nodeClient = buildNodeClient(threadPool);
            action.handleRequest(getCatComponentTemplateRequest, channel, nodeClient);
        }

        // validate results
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertThat(RestResponseUtils.getBodyContent(channel.capturedResponse()).utf8ToString(), equalTo("test_ct 2 0 0 2 0 []\n"));
    }

    public void testRestCatComponentActionWithParam() throws Exception {
        FakeRestRequest getCatComponentTemplateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("_cat/component_templates")
            .withParams(singletonMap("name", "test"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(getCatComponentTemplateRequest, true, 0);

        // execute action
        try (var threadPool = createThreadPool()) {
            final var nodeClient = buildNodeClient(threadPool);
            action.handleRequest(getCatComponentTemplateRequest, channel, nodeClient);
        }

        // validate results
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertThat(channel.capturedResponse().content().utf8ToString(), emptyString());
    }

    public void testActionFailsWithMultipleProjects() throws Exception {
        Metadata metadata = Metadata.builder(clusterState.metadata()).put(ProjectMetadata.builder(randomUniqueProjectId()).build()).build();
        when(clusterState.metadata()).thenReturn(metadata);

        FakeRestRequest getCatComponentTemplateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("_cat/component_templates")
            .build();
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> action.buildTable(getCatComponentTemplateRequest, new ClusterStateResponse(ClusterName.DEFAULT, clusterState, false), "")
        );
        assertThat(ex.getMessage(), containsString("multiple projects"));
    }

    private NoOpNodeClient buildNodeClient(ThreadPool threadPool) {
        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, clusterState, false);

        return new NoOpNodeClient(threadPool) {
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
