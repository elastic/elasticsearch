/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlIndexTemplateRegistryTests extends ESTestCase {

    private final DiscoveryNode node = DiscoveryNodeUtils.create("node");
    private final DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

    private NamedXContentRegistry xContentRegistry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private ArgumentCaptor<PutComposableIndexTemplateAction.Request> putIndexTemplateRequestCaptor;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        doAnswer(withResponse(AcknowledgedResponse.TRUE)).when(indicesAdminClient).putTemplate(any(), any());

        clusterService = mock(ClusterService.class);

        xContentRegistry = new NamedXContentRegistry(
            CollectionUtils.appendToCopy(
                ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse)
            )
        );

        putIndexTemplateRequestCaptor = ArgumentCaptor.forClass(PutComposableIndexTemplateAction.Request.class);
    }

    public void testStatsTemplateWithIlm() {
        MlIndexTemplateRegistry registry = new MlIndexTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            true,
            xContentRegistry
        );

        registry.clusterChanged(createClusterChangedEvent(nodes));

        verify(client, times(4)).execute(same(PutComposableIndexTemplateAction.INSTANCE), putIndexTemplateRequestCaptor.capture(), any());

        PutComposableIndexTemplateAction.Request req = putIndexTemplateRequestCaptor.getAllValues()
            .stream()
            .filter(r -> r.name().equals(MlStatsIndex.TEMPLATE_NAME))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the ml stats index template to be put"));
        ComposableIndexTemplate indexTemplate = req.indexTemplate();
        assertThat(indexTemplate.template().settings().get("index.lifecycle.name"), equalTo("ml-size-based-ilm-policy"));
        assertThat(indexTemplate.template().settings().get("index.lifecycle.rollover_alias"), equalTo(".ml-stats-write"));
    }

    public void testStatsTemplateNoIlm() {
        MlIndexTemplateRegistry registry = new MlIndexTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            false,
            xContentRegistry
        );

        registry.clusterChanged(createClusterChangedEvent(nodes));

        verify(client, times(4)).execute(same(PutComposableIndexTemplateAction.INSTANCE), putIndexTemplateRequestCaptor.capture(), any());

        PutComposableIndexTemplateAction.Request req = putIndexTemplateRequestCaptor.getAllValues()
            .stream()
            .filter(r -> r.name().equals(MlStatsIndex.TEMPLATE_NAME))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the ml stats index template to be put"));
        ComposableIndexTemplate indexTemplate = req.indexTemplate();
        assertThat(indexTemplate.template().settings().get("index.lifecycle.name"), is(nullValue()));
        assertThat(indexTemplate.template().settings().get("index.lifecycle.rollover_alias"), is(nullValue()));
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
            ClusterState.builder(new ClusterName("test")).build()
        );
    }
}
