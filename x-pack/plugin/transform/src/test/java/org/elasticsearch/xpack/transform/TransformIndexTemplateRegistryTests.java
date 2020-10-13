/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class TransformIndexTemplateRegistryTests extends ESTestCase {

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
        doAnswer(withResponse(AcknowledgedResponse.of(true))).when(indicesAdminClient).putTemplate(any(), any());

        clusterService = mock(ClusterService.class);

        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse));
        xContentRegistry = new NamedXContentRegistry(entries);

        putIndexTemplateRequestCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
    }

    public void testTemplates() {
        TransformIndexTemplateRegistry registry =
            new TransformIndexTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);

        registry.clusterChanged(createClusterChangedEvent(nodes));

        verify(client.admin().indices(), times(2)).putTemplate(putIndexTemplateRequestCaptor.capture(), anyObject());

        PutIndexTemplateRequest internal = putIndexTemplateRequestCaptor.getAllValues().stream()
            .filter(r -> r.name().equals(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the transform internal index template to be put"));

        assertEquals(Version.CURRENT.id, internal.version().intValue());
        assertThat(internal.patterns(), Matchers.contains(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME));
        assertThat(internal.settings().get("index.number_of_shards"), equalTo("1"));
        assertThat(internal.settings().get("index.auto_expand_replicas"), equalTo("0-1"));
        assertThat(internal.settings().size(), equalTo(2));

        PutIndexTemplateRequest audit = putIndexTemplateRequestCaptor.getAllValues().stream()
            .filter(r -> r.name().equals(TransformInternalIndexConstants.AUDIT_INDEX))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the transform audit index template to be put"));

        assertEquals(Version.CURRENT.id, audit.version().intValue());
        assertThat(audit.patterns(), Matchers.contains(TransformInternalIndexConstants.AUDIT_INDEX_PREFIX + "*"));
        Alias alias = audit.aliases().iterator().next();
        assertThat(alias.name(), equalTo(".transform-notifications-read"));
        assertTrue(alias.isHidden());
        assertThat(audit.settings().get("index.number_of_shards"), equalTo("1"));
        assertThat(audit.settings().get("index.auto_expand_replicas"), equalTo("0-1"));
        assertThat(audit.settings().get("index.hidden"), equalTo("true"));
        assertThat(audit.settings().size(), equalTo(3));
    }

    public void testRefactoredMappingsAreSameAsOld() throws IOException {
        TransformIndexTemplateRegistry registry =
            new TransformIndexTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);
        {
            IndexTemplateConfig auditConfig = registry.getLegacyTemplateConfigs().stream()
                .filter(r -> r.getTemplateName().equals(TransformInternalIndexConstants.AUDIT_INDEX))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected the transform audit index template to be put"));

            byte[] bytes = auditConfig.loadBytes();
            Map<String, Object> auditTemplate =
                XContentHelper.convertToMap(new BytesArray(bytes, 0, bytes.length), true, XContentType.JSON).v2();

            Map<String, Object> oldMappings =
                XContentHelper.convertToMap(BytesReference.bytes(TransformInternalIndex.auditMappings()), true, XContentType.JSON).v2();

            assertEquals(oldMappings, auditTemplate.get("mappings"));
        }

        {
            IndexTemplateConfig internalConfig = registry.getLegacyTemplateConfigs().stream()
                .filter(r -> r.getTemplateName().equals(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected the transform internal index template to be put"));

            byte[] bytes = internalConfig.loadBytes();
            Map<String, Object> internalTemplate =
                XContentHelper.convertToMap(new BytesArray(bytes, 0, bytes.length), true, XContentType.JSON).v2();

            Map<String, Object> oldMappings =
                XContentHelper.convertToMap(BytesReference.bytes(TransformInternalIndex.mappings()), true,
                    XContentType.JSON).v2();

            assertEquals(oldMappings, internalTemplate.get("mappings"));
        }
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
