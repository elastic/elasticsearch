/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealmMigrator;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.xpack.security.SecurityTemplateService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.SecurityTemplateService.SECURITY_INDEX_TEMPLATE_VERSION_PATTERN;
import static org.elasticsearch.xpack.security.SecurityTemplateService.SECURITY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.security.SecurityTemplateService.UpgradeState;
import static org.elasticsearch.xpack.security.SecurityTemplateService.securityIndexMappingVersionMatches;
import static org.elasticsearch.xpack.security.SecurityTemplateService.securityTemplateExistsAndVersionMatches;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityTemplateServiceTests extends ESTestCase {
    private InternalClient client;
    private TransportClient transportClient;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private NativeRealmMigrator nativeRealmMigrator;
    SecurityTemplateService securityTemplateService;
    private static final ClusterState EMPTY_CLUSTER_STATE =
            new ClusterState.Builder(new ClusterName("test-cluster")).build();

    CopyOnWriteArrayList<ActionListener> listeners;

    @Before
    public void setup() {
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(buildNewFakeTransportAddress().toString());
        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);

        threadPool = new TestThreadPool("security template service tests");
        transportClient = new MockTransportClient(Settings.EMPTY);
        class IClient extends InternalClient {
            IClient(Client transportClient) {
                super(Settings.EMPTY, null, transportClient, null);
            }

            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
                    ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                    Action<Request, Response, RequestBuilder> action, Request request
                    , ActionListener<Response> listener) {
                listeners.add(listener);
            }
        }

        nativeRealmMigrator = mock(NativeRealmMigrator.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(false);
            return null;
        }).when(nativeRealmMigrator).performUpgrade(any(Version.class), any(ActionListener.class));

        client = new IClient(transportClient);
        securityTemplateService = new SecurityTemplateService(Settings.EMPTY, client, nativeRealmMigrator);
        listeners = new CopyOnWriteArrayList<>();
    }

    @After
    public void stop() throws InterruptedException {
        if (transportClient != null) {
            transportClient.close();
        }
        terminate(threadPool);
    }

    public void testIndexTemplateIsIdentifiedAsUpToDate() throws IOException {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        assertTrue(SecurityTemplateService.securityTemplateExistsAndIsUpToDate(clusterStateBuilder.build(), logger));
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(0));
    }

    public void testFaultyIndexTemplateIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/wrong-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        assertFalse(SecurityTemplateService.securityTemplateExistsAndIsUpToDate(clusterStateBuilder.build(), logger));
        checkTemplateUpdateWorkCorrectly(clusterStateBuilder);
    }

    public void testIndexTemplateVersionMatching() throws Exception {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        assertTrue(securityTemplateExistsAndVersionMatches(clusterStateBuilder.build(), logger, Version.V_5_0_0::before));
        assertFalse(securityTemplateExistsAndVersionMatches(clusterStateBuilder.build(), logger, Version.V_5_0_0::after));
    }

    private void checkTemplateUpdateWorkCorrectly(ClusterState.Builder clusterStateBuilder) throws IOException {
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(1));
        assertTrue(securityTemplateService.templateCreationPending.get());

        // if we do it again this should not send an update
        ActionListener listener = listeners.get(0);
        listeners.clear();
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(0));
        assertTrue(securityTemplateService.templateCreationPending.get());

        // if we now simulate an error...
        listener.onFailure(new Exception());
        assertFalse(securityTemplateService.templateCreationPending.get());

        // ... we should be able to send a new update
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(1));
        assertTrue(securityTemplateService.templateCreationPending.get());

        // now check what happens if we get back an unacknowledged response
        try {
            listeners.get(0).onResponse(new TestPutIndexTemplateResponse());
            fail("this should have failed because request was not acknowledged");
        } catch (ElasticsearchException e) {
        }
        assertFalse(securityTemplateService.updateMappingPending.get());

        // and now let's see what happens if we get back a response
        listeners.clear();
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertTrue(securityTemplateService.templateCreationPending.get());
        assertThat(listeners.size(), equalTo(1));
        listeners.get(0).onResponse(new TestPutIndexTemplateResponse(true));
        assertFalse(securityTemplateService.templateCreationPending.get());
    }

    public void testMissingIndexTemplateIsIdentifiedAsMissing() throws IOException {
        ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(state());
        // add the correct mapping
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexMetaData.Builder indexMeta = createIndexMetadata(mappingString);
        MetaData.Builder builder = new MetaData.Builder(clusterStateBuilder.build().getMetaData());
        builder.put(indexMeta);
        clusterStateBuilder.metaData(builder);
        assertFalse(SecurityTemplateService.securityTemplateExistsAndIsUpToDate(clusterStateBuilder.build(), logger));
        checkTemplateUpdateWorkCorrectly(clusterStateBuilder);
    }

    public void testMissingVersionIndexTemplateIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/missing-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        assertFalse(SecurityTemplateService.securityTemplateExistsAndIsUpToDate(clusterStateBuilder.build(), logger));
        checkTemplateUpdateWorkCorrectly(clusterStateBuilder);
    }

    public void testOutdatedMappingIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/wrong-version-" + SECURITY_TEMPLATE_NAME + ".json";
        final Version wrongVersion = Version.fromString("4.0.0");
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMapping(templateString);
        assertFalse(SecurityTemplateService.securityIndexMappingUpToDate(clusterStateBuilder.build(), logger));
        assertThat(SecurityTemplateService.oldestSecurityIndexMappingVersion(clusterStateBuilder.build(), logger), equalTo(wrongVersion));
        checkMappingUpdateWorkCorrectly(clusterStateBuilder, wrongVersion);
    }

    private void checkMappingUpdateWorkCorrectly(ClusterState.Builder clusterStateBuilder, Version expectedOldVersion) {
        AtomicReference<Version> migratorVersionRef = new AtomicReference<>(null);
        AtomicReference<ActionListener<Boolean>> migratorListenerRef = new AtomicReference<>(null);
        Mockito.doAnswer(invocation -> {
            migratorVersionRef.set((Version) invocation.getArguments()[0]);
            migratorListenerRef.set((ActionListener<Boolean>) invocation.getArguments()[1]);
            return null;
        }).when(nativeRealmMigrator).performUpgrade(any(Version.class), any(ActionListener.class));

        assertThat(securityTemplateService.upgradeDataState.get(), equalTo(UpgradeState.NOT_STARTED));

        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build(),
                EMPTY_CLUSTER_STATE));

        assertThat(migratorVersionRef.get(), equalTo(expectedOldVersion));
        assertThat(migratorListenerRef.get(), notNullValue());
        assertThat(listeners.size(), equalTo(0)); // migrator has not responded yet
        assertThat(securityTemplateService.updateMappingPending.get(), equalTo(false));
        assertThat(securityTemplateService.upgradeDataState.get(), equalTo(UpgradeState.IN_PROGRESS));

        migratorListenerRef.get().onResponse(true);

        assertThat(listeners.size(), equalTo(3)); // we have three types in the mapping
        assertTrue(securityTemplateService.updateMappingPending.get());
        assertThat(securityTemplateService.upgradeDataState.get(), equalTo(UpgradeState.COMPLETE));

        // if we do it again this should not send an update
        ActionListener listener = listeners.get(0);
        listeners.clear();
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(0));
        assertTrue(securityTemplateService.updateMappingPending.get());

        // if we now simulate an error...
        listener.onFailure(new Exception("Testing failure handling"));
        assertFalse(securityTemplateService.updateMappingPending.get());

        // ... we should be able to send a new update
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(3));
        assertTrue(securityTemplateService.updateMappingPending.get());

        // now check what happens if we get back an unacknowledged response
        try {
            listeners.get(0).onResponse(new TestPutMappingResponse());
            fail("this hould have failed because request was not acknowledged");
        } catch (ElasticsearchException e) {
        }
        assertFalse(securityTemplateService.updateMappingPending.get());

        // and now check what happens if we get back an acknowledged response
        listeners.clear();
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(3)); // we have three types in the mapping
        int counter = 0;
        for (ActionListener actionListener : listeners) {
            actionListener.onResponse(new TestPutMappingResponse(true));
            if (counter++ < 2) {
                assertTrue(securityTemplateService.updateMappingPending.get());
            } else {
                assertFalse(securityTemplateService.updateMappingPending.get());
            }
        }
    }

    public void testUpToDateMappingIsIdentifiedAstUpToDate() throws IOException {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMapping(templateString);
        assertTrue(SecurityTemplateService.securityIndexMappingUpToDate(clusterStateBuilder.build(), logger));
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(0));
    }

    public void testMappingVersionMatching() throws IOException {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMapping(templateString);
        assertTrue(securityIndexMappingVersionMatches(clusterStateBuilder.build(), logger, Version.V_5_0_0::before));
        assertFalse(securityIndexMappingVersionMatches(clusterStateBuilder.build(), logger, Version.V_5_0_0::after));
    }

    public void testMissingVersionMappingIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/missing-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMapping(templateString);
        assertFalse(SecurityTemplateService.securityIndexMappingUpToDate(clusterStateBuilder.build(), logger));
        assertThat(SecurityTemplateService.oldestSecurityIndexMappingVersion(clusterStateBuilder.build(), logger),
                equalTo(Version.V_2_3_0));
        checkMappingUpdateWorkCorrectly(clusterStateBuilder, Version.V_2_3_0);
    }

    public void testMissingIndexIsIdentifiedAsUpToDate() throws IOException {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(new ClusterName("test-cluster"));
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexTemplateMetaData.Builder templateMeta = getIndexTemplateMetaData(mappingString);
        MetaData.Builder builder = new MetaData.Builder(clusterStateBuilder.build().getMetaData());
        builder.put(templateMeta);
        clusterStateBuilder.metaData(builder);
        assertTrue(SecurityTemplateService.securityIndexMappingUpToDate(clusterStateBuilder.build(), logger));
        securityTemplateService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertThat(SecurityTemplateService.oldestSecurityIndexMappingVersion(clusterStateBuilder.build(), logger), nullValue());
        assertThat(listeners.size(), equalTo(0));
    }

    private ClusterState.Builder createClusterStateWithMapping(String templateString) throws IOException {
        IndexMetaData.Builder indexMetaData = createIndexMetadata(templateString);
        ImmutableOpenMap.Builder mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(SECURITY_INDEX_NAME, indexMetaData.build());
        MetaData.Builder metaDataBuilder = new MetaData.Builder();
        metaDataBuilder.indices(mapBuilder.build());
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexTemplateMetaData.Builder templateMeta = getIndexTemplateMetaData(mappingString);
        metaDataBuilder.put(templateMeta);
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(state());
        clusterStateBuilder.metaData(metaDataBuilder.build()).routingTable(SecurityTestUtils.buildSecurityIndexRoutingTable());
        return clusterStateBuilder;
    }

    private IndexMetaData.Builder createIndexMetadata(String templateString) throws IOException {
        String template = TemplateUtils.loadTemplate(templateString, Version.CURRENT.toString()
                , SECURITY_INDEX_TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(SECURITY_INDEX_NAME);
        indexMetaData.settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .build());

        for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
            indexMetaData.putMapping(entry.getKey(), entry.getValue());
        }
        return indexMetaData;
    }

    private ClusterState.Builder createClusterStateWithTemplate(String templateString) throws IOException {
        IndexTemplateMetaData.Builder templateBuilder = getIndexTemplateMetaData(templateString);
        MetaData.Builder metaDataBuidler = new MetaData.Builder();
        metaDataBuidler.put(templateBuilder);
        // add the correct mapping no matter what the template
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexMetaData.Builder indexMeta = createIndexMetadata(mappingString);
        metaDataBuidler.put(indexMeta);
        return ClusterState.builder(state())
                .metaData(metaDataBuidler.build());
    }

    private IndexTemplateMetaData.Builder getIndexTemplateMetaData(String templateString) throws IOException {
        String template = TemplateUtils.loadTemplate(templateString, Version.CURRENT.toString()
                , SECURITY_INDEX_TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(SECURITY_TEMPLATE_NAME);
        for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
            templateBuilder.putMapping(entry.getKey(), entry.getValue());
        }
        return templateBuilder;
    }

    // cluster state where local node is master
    private static ClusterState state() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        discoBuilder.masterNodeId("1");
        discoBuilder.localNodeId("1");
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test-cluster"));
        state.nodes(discoBuilder);
        state.metaData(MetaData.builder().generateClusterUuidIfNeeded());
        return state.build();
    }

    private static class TestPutMappingResponse extends PutMappingResponse {
        TestPutMappingResponse(boolean acknowledged) {
            super(acknowledged);
        }

        TestPutMappingResponse() {
            super();
        }
    }

    private static class TestPutIndexTemplateResponse extends PutIndexTemplateResponse {
        TestPutIndexTemplateResponse(boolean acknowledged) {
            super(acknowledged);
        }

        TestPutIndexTemplateResponse() {
            super();
        }
    }
}
