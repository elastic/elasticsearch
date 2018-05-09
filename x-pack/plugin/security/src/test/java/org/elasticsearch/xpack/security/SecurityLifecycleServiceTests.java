/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
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
import org.elasticsearch.xpack.core.security.SecurityLifecycleServiceField;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.SecurityLifecycleServiceField.SECURITY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.securityIndexMappingUpToDate;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityLifecycleServiceTests extends ESTestCase {
    private TransportClient transportClient;
    private ThreadPool threadPool;
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
        Client client = new FilterClient(transportClient) {
            @Override
            protected <Request extends ActionRequest,
                    Response extends ActionResponse,
                    RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
            void doExecute(Action<Request, Response, RequestBuilder> action, Request request,
                           ActionListener<Response> listener) {
                listeners.add(listener);
            }
        };
        securityLifecycleService = new SecurityLifecycleService(Settings.EMPTY, clusterService,
                threadPool, client, mock(IndexAuditTrail.class));
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
        // No upgrade actions run
        assertThat(listeners.size(), equalTo(0));
    }

    public void testIndexTemplateVersionMatching() throws Exception {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();

        assertTrue(SecurityIndexManager.checkTemplateExistsAndVersionMatches(
                SecurityLifecycleServiceField.SECURITY_TEMPLATE_NAME, clusterState, logger,
                Version.V_5_0_0::before));
        assertFalse(SecurityIndexManager.checkTemplateExistsAndVersionMatches(
                SecurityLifecycleServiceField.SECURITY_TEMPLATE_NAME, clusterState, logger,
                Version.V_5_0_0::after));
    }

    public void testUpToDateMappingsAreIdentifiedAsUpToDate() throws IOException {
        String securityTemplateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(securityTemplateString);
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(listeners.size(), equalTo(0));
    }

    public void testMappingVersionMatching() throws IOException {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        securityLifecycleService.clusterChanged(new ClusterChangedEvent("test-event",
                clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        final SecurityIndexManager securityIndex = securityLifecycleService.securityIndex();
        assertTrue(securityIndex.checkMappingVersion(Version.V_5_0_0::before));
        assertFalse(securityIndex.checkMappingVersion(Version.V_5_0_0::after));
    }

    public void testMissingVersionMappingThrowsError() throws IOException {
        String templateString = "/missing-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();
        IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> securityIndexMappingUpToDate(clusterState, logger));
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
                SecurityIndexManager.TEMPLATE_VERSION_PATTERN);
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
                SecurityIndexManager.TEMPLATE_VERSION_PATTERN);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(templateName)
                .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)));
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
}
