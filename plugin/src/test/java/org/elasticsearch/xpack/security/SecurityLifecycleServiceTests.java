/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealmMigrator;
import org.elasticsearch.xpack.security.support.IndexLifecycleManager;
import org.elasticsearch.xpack.security.support.IndexLifecycleManager.UpgradeState;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.securityIndexMappingAndTemplateSufficientToRead;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.securityIndexMappingAndTemplateUpToDate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityLifecycleServiceTests extends ESTestCase {
    private TransportClient transportClient;
    private ThreadPool threadPool;
    private NativeRealmMigrator nativeRealmMigrator;
    private SecurityLifecycleService securityLifecycleService;
    private static final ClusterState EMPTY_CLUSTER_STATE =
            new ClusterState.Builder(new ClusterName("test-cluster")).build();
    private CopyOnWriteArrayList<ActionListener> listeners;

    @Before
    public void setup() {
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(buildNewFakeTransportAddress().toString());
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);

        threadPool = new TestThreadPool("security template service tests");
        transportClient = new MockTransportClient(Settings.EMPTY);
        class IClient extends InternalClient {
            IClient(Client transportClient) {
                super(Settings.EMPTY, null, transportClient);
            }

            @Override
            protected <Request extends ActionRequest,
                    Response extends ActionResponse,
                    RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
            void doExecute(Action<Request, Response, RequestBuilder> action, Request request,
                           ActionListener<Response> listener) {
                listeners.add(listener);
            }
        }

        nativeRealmMigrator = mock(NativeRealmMigrator.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener) invocation.getArguments()[1];
            listener.onResponse(false);
            return null;
        }).when(nativeRealmMigrator).performUpgrade(any(Version.class), any(ActionListener.class));

        InternalClient client = new IClient(transportClient);
        securityLifecycleService = new SecurityLifecycleService(Settings.EMPTY, clusterService,
                threadPool, client, nativeRealmMigrator, mock(IndexAuditTrail.class));
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
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(
                "/" + SECURITY_TEMPLATE_NAME + ".json"
        );
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(true));
        // No upgrade actions run
        assertThat(listeners.size(), equalTo(0));
    }

    public void testFaultyIndexTemplateIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/wrong-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        checkTemplateUpdateWorkCorrectly(clusterStateBuilder);
    }

    public void testIndexTemplateVersionMatching() throws Exception {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();

        assertTrue(IndexLifecycleManager.checkTemplateExistsAndVersionMatches(
                SecurityLifecycleService.SECURITY_TEMPLATE_NAME, clusterState, logger,
                Version.V_5_0_0::before));
        assertFalse(IndexLifecycleManager.checkTemplateExistsAndVersionMatches(
                SecurityLifecycleService.SECURITY_TEMPLATE_NAME, clusterState, logger,
                Version.V_5_0_0::after));
    }

    private void checkTemplateUpdateWorkCorrectly(ClusterState.Builder clusterStateBuilder)
            throws IOException {

        final int numberOfSecurityIndices = 1; // .security

        final ClusterState clusterState = clusterStateBuilder.build();
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterState, EMPTY_CLUSTER_STATE));
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(false));
        assertThat(listeners.size(), equalTo(numberOfSecurityIndices));
        assertTrue(securityLifecycleService.securityIndex().isTemplateCreationPending());

        // if we do it again this should not send an update
        ActionListener listener = listeners.get(0);
        listeners.clear();
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterState, EMPTY_CLUSTER_STATE));
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(false));
        assertThat(listeners.size(), equalTo(0));
        assertTrue(securityLifecycleService.securityIndex().isTemplateCreationPending());

        // if we now simulate an error...
        listener.onFailure(new Exception());
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(false));
        assertFalse(securityLifecycleService.securityIndex().isTemplateCreationPending());

        // ... we should be able to send a new update
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterState, EMPTY_CLUSTER_STATE));
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(false));
        assertThat(listeners.size(), equalTo(1));
        assertTrue(securityLifecycleService.securityIndex().isTemplateCreationPending());

        // now check what happens if we get back an unacknowledged response
        expectThrows(ElasticsearchException.class,
                () -> listeners.get(0).onResponse(new TestPutIndexTemplateResponse())
        );
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(false));
        assertFalse(securityLifecycleService.securityIndex().isTemplateCreationPending());

        // and now let's see what happens if we get back a response
        listeners.clear();
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterState, EMPTY_CLUSTER_STATE));
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(false));
        assertTrue(securityLifecycleService.securityIndex().isTemplateCreationPending());
        assertThat(listeners.size(), equalTo(1));
        listeners.get(0).onResponse(new TestPutIndexTemplateResponse(true));
        assertThat(securityLifecycleService.securityIndex().isTemplateUpToDate(), equalTo(true));
        assertFalse(securityLifecycleService.securityIndex().isTemplateCreationPending());
    }

    public void testMissingIndexTemplateIsIdentifiedAsMissing() throws IOException {
        // add the correct mapping
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMapping(mappingString);
        checkTemplateUpdateWorkCorrectly(clusterStateBuilder);
    }

    public void testMissingVersionIndexTemplateIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/missing-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        checkTemplateUpdateWorkCorrectly(clusterStateBuilder);
    }

    public void testOutdatedMappingIsIdentifiedAsNotUpToDate() throws IOException {
        String templateString = "/wrong-version-" + SECURITY_TEMPLATE_NAME + ".json";
        final Version wrongVersion = Version.fromString("4.0.0");
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();
        assertFalse(securityIndexMappingAndTemplateUpToDate(clusterState, logger));
        assertFalse(securityIndexMappingAndTemplateSufficientToRead(clusterState, logger));
        checkMappingUpdateWorkCorrectly(clusterStateBuilder, wrongVersion);
    }

    private void checkMappingUpdateWorkCorrectly(ClusterState.Builder clusterStateBuilder, Version expectedOldVersion) {
        final int totalNumberOfTypes = 1;

        AtomicReference<Version> migratorVersionRef = new AtomicReference<>(null);
        AtomicReference<ActionListener<Boolean>> migratorListenerRef = new AtomicReference<>(null);
        Mockito.doAnswer(invocation -> {
            migratorVersionRef.set((Version) invocation.getArguments()[0]);
            migratorListenerRef.set((ActionListener<Boolean>) invocation.getArguments()[1]);
            return null;
        }).when(nativeRealmMigrator).performUpgrade(any(Version.class), any(ActionListener.class));

        final IndexLifecycleManager securityIndex = securityLifecycleService.securityIndex();
        assertThat(securityIndex.getMigrationState(), equalTo(UpgradeState.NOT_STARTED));

        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));

        assertThat(migratorVersionRef.get(), equalTo(expectedOldVersion));
        assertThat(migratorListenerRef.get(), notNullValue());

        // security migrator has not responded yet
        assertThat(this.listeners.size(), equalTo(0));

        assertThat(securityIndex.isMappingUpdatePending(), equalTo(false));
        assertThat(securityIndex.getMigrationState(), equalTo(UpgradeState.IN_PROGRESS));

        migratorListenerRef.get().onResponse(true);

        assertThat(this.listeners, iterableWithSize(totalNumberOfTypes));
        assertThat(securityIndex.isMappingUpdatePending(), equalTo(true));
        assertThat(securityIndex.getMigrationState(), equalTo(UpgradeState.COMPLETE));

        // if we do it again this should not send an update
        List<ActionListener> cloneListeners = new ArrayList<>(this.listeners);
        this.listeners.clear();
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(this.listeners.size(), equalTo(0));
        assertThat(securityIndex.isMappingUpdatePending(), equalTo(true));

        // if we now simulate an error...
        cloneListeners.forEach(l -> l.onFailure(new Exception("Testing failure handling")));
        assertThat(securityIndex.isMappingUpdatePending(), equalTo(false));

        // ... we should be able to send a new update
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(this.listeners.size(), equalTo(totalNumberOfTypes));
        assertThat(securityIndex.isMappingUpdatePending(), equalTo(true));

        // now check what happens if we get back an unacknowledged response
        try {
            this.listeners.get(0).onResponse(new TestPutMappingResponse());
            fail("this should have failed because request was not acknowledged");
        } catch (ElasticsearchException e) {
        }
        assertThat(securityIndex.isMappingUpdatePending(), equalTo(false));

        // and now check what happens if we get back an acknowledged response
        this.listeners.clear();
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(this.listeners.size(), equalTo(totalNumberOfTypes));
        int counter = 0;
        for (ActionListener actionListener : this.listeners) {
            actionListener.onResponse(new TestPutMappingResponse(true));
            if (++counter < totalNumberOfTypes) {
                assertThat(securityIndex.isMappingUpdatePending(), equalTo(true));
            } else {
                assertThat(securityIndex.isMappingUpdatePending(), equalTo(false));
            }
        }
    }

    public void testUpToDateMappingsAreIdentifiedAsUpToDate() throws IOException {
        String securityTemplateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(securityTemplateString);
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertTrue(securityLifecycleService.securityIndex().isMappingUpToDate());
        assertThat(listeners.size(), equalTo(0));
    }

    public void testMappingVersionMatching() throws IOException {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        final IndexLifecycleManager securityIndex = securityLifecycleService.securityIndex();
        assertTrue(securityIndex.checkMappingVersion(Version.V_5_0_0::before));
        assertFalse(securityIndex.checkMappingVersion(Version.V_5_0_0::after));
    }

    public void testMissingVersionMappingThrowsError() throws IOException {
        String templateString = "/missing-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();
        IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> securityIndexMappingAndTemplateUpToDate(clusterState, logger));
        assertEquals("Cannot read security-version string in index " + SECURITY_INDEX_NAME,
            exception.getMessage());
    }

    public void testMissingIndexIsIdentifiedAsUpToDate() throws IOException {
        final ClusterName clusterName = new ClusterName("test-cluster");
        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(clusterName);
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexTemplateMetaData.Builder templateMeta = getIndexTemplateMetaData(SECURITY_TEMPLATE_NAME, mappingString);
        MetaData.Builder builder = new MetaData.Builder(clusterStateBuilder.build().getMetaData());
        builder.put(templateMeta);
        clusterStateBuilder.metaData(builder);
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
                , EMPTY_CLUSTER_STATE));
        assertTrue(securityLifecycleService.securityIndex().isMappingUpToDate());
        assertThat(securityLifecycleService.securityIndex().getMappingVersion(), nullValue());
        assertThat(listeners.size(), equalTo(0));
    }

    private ClusterState.Builder createClusterStateWithMapping(String securityTemplateString) throws IOException {
        final ClusterState clusterState = createClusterStateWithIndex(securityTemplateString).build();
        final String indexName = clusterState.metaData().getAliasAndIndexLookup()
            .get(SECURITY_INDEX_NAME).getIndices().get(0).getIndex().getName();
        return ClusterState.builder(clusterState).routingTable(SecurityTestUtils.buildIndexRoutingTable(indexName));
    }

    private ClusterState.Builder createClusterStateWithMappingAndTemplate(String securityTemplateString) throws IOException {
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMapping(securityTemplateString);
        MetaData.Builder metaDataBuilder = new MetaData.Builder(clusterStateBuilder.build().metaData());
        String securityMappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexTemplateMetaData.Builder securityTemplateMeta = getIndexTemplateMetaData(SECURITY_TEMPLATE_NAME, securityMappingString);
        metaDataBuilder.put(securityTemplateMeta);
        return clusterStateBuilder.metaData(metaDataBuilder);
    }

    private static IndexMetaData.Builder createIndexMetadata(String indexName, String templateString) throws IOException {
        String template = TemplateUtils.loadTemplate(templateString, Version.CURRENT.toString(),
                IndexLifecycleManager.TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
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

    public ClusterState.Builder createClusterStateWithTemplate(String securityTemplateString) throws IOException {
        // add the correct mapping no matter what the template
        ClusterState clusterState = createClusterStateWithIndex("/" + SECURITY_TEMPLATE_NAME + ".json").build();
        final MetaData.Builder metaDataBuilder = new MetaData.Builder(clusterState.metaData());
        metaDataBuilder.put(getIndexTemplateMetaData(SECURITY_TEMPLATE_NAME, securityTemplateString));
        return ClusterState.builder(clusterState).metaData(metaDataBuilder);
    }

    private ClusterState.Builder createClusterStateWithIndex(String securityTemplate) throws IOException {
        final MetaData.Builder metaDataBuilder = new MetaData.Builder();
        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_INDEX_NAME + (withAlias ? "-" + randomAlphaOfLength(5) : "");
        metaDataBuilder.put(createIndexMetadata(securityIndexName, securityTemplate));

        ClusterState.Builder clusterStateBuilder = ClusterState.builder(state());
        if (withAlias) {
            // try with .security index as an alias
            clusterStateBuilder.metaData(SecurityTestUtils.addAliasToMetaData(metaDataBuilder.build(), securityIndexName));
        } else {
            // try with .security index as a concrete index
            clusterStateBuilder.metaData(metaDataBuilder);
        }

        clusterStateBuilder.routingTable(SecurityTestUtils.buildIndexRoutingTable(securityIndexName));
        return clusterStateBuilder;
    }

    private static IndexTemplateMetaData.Builder getIndexTemplateMetaData(
            String templateName, String templateString) throws IOException {

        String template = TemplateUtils.loadTemplate(templateString, Version.CURRENT.toString(),
                IndexLifecycleManager.TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexTemplateMetaData.Builder templateBuilder =
                IndexTemplateMetaData.builder(templateName);
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
