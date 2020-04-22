/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class MlIndexTemplateRegistryTests extends ESTestCase {

    private final DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
    private final DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

    private NamedXContentRegistry xContentRegistry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private ArgumentCaptor<PutIndexTemplateRequest> putIndexTemplateRequestCaptor;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.newDirectExecutorService());

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        doAnswer(withResponse(new AcknowledgedResponse(true))).when(indicesAdminClient).putTemplate(any(), any());

        clusterService = mock(ClusterService.class);

        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse));
        xContentRegistry = new NamedXContentRegistry(entries);

        putIndexTemplateRequestCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
    }

    public void testStateTemplate() {
        MlIndexTemplateRegistry registry =
            new MlIndexTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);

        registry.clusterChanged(createClusterChangedEvent(nodes));

        verify(client.admin().indices(), times(7)).putTemplate(putIndexTemplateRequestCaptor.capture(), anyObject());

        PutIndexTemplateRequest req = putIndexTemplateRequestCaptor.getAllValues().stream()
            .filter(r -> r.name().equals(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the ml state index template to be put"));
        assertThat(req.settings().get("index.lifecycle.name"), equalTo("ml-size-based-ilm-policy"));
        assertThat(req.settings().get("index.lifecycle.rollover_alias"), equalTo(".ml-state-write"));
    }

    public void testStatsTemplate() {
        MlIndexTemplateRegistry registry =
            new MlIndexTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);

        registry.clusterChanged(createClusterChangedEvent(nodes));

        verify(client.admin().indices(), times(7)).putTemplate(putIndexTemplateRequestCaptor.capture(), anyObject());

        PutIndexTemplateRequest req = putIndexTemplateRequestCaptor.getAllValues().stream()
            .filter(r -> r.name().equals(MlStatsIndex.TEMPLATE_NAME))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the ml stats index template to be put"));
        assertThat(req.settings().get("index.lifecycle.name"), equalTo("ml-size-based-ilm-policy"));
        assertThat(req.settings().get("index.lifecycle.rollover_alias"), equalTo(".ml-stats-write"));
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[1];
            listener.onResponse(response);
            return null;
        };
    }

    private static ClusterChangedEvent createClusterChangedEvent(DiscoveryNodes nodes) {
        return new ClusterChangedEvent(
            "created-from-test",
            ClusterState.builder(new ClusterName("test")).nodes(nodes).build(),
            ClusterState.builder(new ClusterName("test")).build());
    }
}
