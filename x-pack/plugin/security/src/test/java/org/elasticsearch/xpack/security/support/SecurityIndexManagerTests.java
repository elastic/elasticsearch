/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.hamcrest.Matchers;
import org.junit.Before;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.TEMPLATE_VERSION_PATTERN;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityIndexManagerTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("security-index-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();
    private static final String INDEX_NAME = ".security";
    private static final String TEMPLATE_NAME = "SecurityIndexManagerTests-template";
    private SecurityIndexManager manager;
    private Map<Action<?>, Map<ActionRequest, ActionListener<?>>> actions;

    @Before
    public void setUpManager() {
        final Client mockClient = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        final ClusterService clusterService = mock(ClusterService.class);

        actions = new LinkedHashMap<>();
        final Client client = new FilterClient(mockClient) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                final Map<ActionRequest, ActionListener<?>> map = actions.getOrDefault(action, new HashMap<>());
                map.put(request, listener);
                actions.put(action, map);
            }
        };
        manager = new SecurityIndexManager(client, INDEX_NAME, clusterService);
    }

    public void testIndexWithUpToDateMappingAndTemplate() throws IOException {
        assertInitialState();

        final ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(true));
        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(true));
    }

    public void testIndexWithoutPrimaryShards() throws IOException {
        assertInitialState();

        final ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        Index index = new Index(INDEX_NAME, UUID.randomUUID().toString());
        ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId(index, 0), true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize())
                        .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "")))
                .build();
        clusterStateBuilder.routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(index).addIndexShard(table).build())
                .build());
        manager.clusterChanged(event(clusterStateBuilder));

        assertIndexUpToDateButNotAvailable();
    }

    private ClusterChangedEvent event(ClusterState.Builder clusterStateBuilder) {
        return new ClusterChangedEvent("test-event", clusterStateBuilder.build(), EMPTY_CLUSTER_STATE);
    }

    public void testIndexHealthChangeListeners() throws Exception {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        final AtomicReference<SecurityIndexManager.State> previousState = new AtomicReference<>();
        final AtomicReference<SecurityIndexManager.State> currentState = new AtomicReference<>();
        final BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> listener = (prevState, state) -> {
            previousState.set(prevState);
            currentState.set(state);
            listenerCalled.set(true);
        };
        manager.addIndexStateListener(listener);

        // index doesn't exist and now exists
        final ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertTrue(listenerCalled.get());
        assertNull(previousState.get().indexStatus);
        assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexStatus);

        // reset and call with no change to the index
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        ClusterChangedEvent event = new ClusterChangedEvent("same index health", clusterStateBuilder.build(), clusterStateBuilder.build());
        manager.clusterChanged(event);

        assertFalse(listenerCalled.get());
        assertNull(previousState.get());
        assertNull(currentState.get());

        // index with different health
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        ClusterState previousClusterState = clusterStateBuilder.build();
        Index prevIndex = previousClusterState.getRoutingTable().index(INDEX_NAME).getIndex();
        clusterStateBuilder.routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(prevIndex)
                        .addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(prevIndex, 0))
                                .addShard(ShardRouting.newUnassigned(new ShardId(prevIndex, 0), true,
                                    RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""))
                                        .initialize(UUIDs.randomBase64UUID(random()), null, 0L)
                                        .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "")))
                                .build()))
                .build());



        event = new ClusterChangedEvent("different index health", clusterStateBuilder.build(), previousClusterState);
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertEquals(ClusterHealthStatus.GREEN, previousState.get().indexStatus);
        assertEquals(ClusterHealthStatus.RED, currentState.get().indexStatus);

        // swap prev and current
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        event = new ClusterChangedEvent("different index health swapped", previousClusterState, clusterStateBuilder.build());
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertEquals(ClusterHealthStatus.RED, previousState.get().indexStatus);
        assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexStatus);
    }

    public void testIndexOutOfDateListeners() throws Exception {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME)));
        AtomicBoolean upToDateChanged = new AtomicBoolean();
        manager.addIndexStateListener((prev, current) -> {
            listenerCalled.set(true);
            upToDateChanged.set(prev.isIndexUpToDate != current.isIndexUpToDate);
        });
        assertTrue(manager.isIndexUpToDate());

        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME)));
        assertFalse(listenerCalled.get());
        assertTrue(manager.isIndexUpToDate());

        // index doesn't exist and now exists with wrong format
        ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME,
                SecurityIndexManager.INTERNAL_INDEX_FORMAT - 1);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));
        assertTrue(listenerCalled.get());
        assertTrue(upToDateChanged.get());
        assertFalse(manager.isIndexUpToDate());

        listenerCalled.set(false);
        assertFalse(listenerCalled.get());
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME)));
        assertTrue(listenerCalled.get());
        assertTrue(upToDateChanged.get());
        assertTrue(manager.isIndexUpToDate());

        listenerCalled.set(false);
        // index doesn't exist and now exists with correct format
        clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME, SecurityIndexManager.INTERNAL_INDEX_FORMAT);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));
        assertTrue(listenerCalled.get());
        assertFalse(upToDateChanged.get());
        assertTrue(manager.isIndexUpToDate());
    }

    private void assertInitialState() {
        assertThat(manager.indexExists(), Matchers.equalTo(false));
        assertThat(manager.isAvailable(), Matchers.equalTo(false));
        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(false));
    }

    private void assertIndexUpToDateButNotAvailable() {
        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(false));
        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(true));
    }

    public static ClusterState.Builder createClusterState(String indexName, String templateName) throws IOException {
        return createClusterState(indexName, templateName, templateName, SecurityIndexManager.INTERNAL_INDEX_FORMAT);
    }

    public static ClusterState.Builder createClusterState(String indexName, String templateName, int format) throws IOException {
        return createClusterState(indexName, templateName, templateName, format);
    }

    private static ClusterState.Builder createClusterState(String indexName, String templateName, String buildMappingFrom, int format)
            throws IOException {
        IndexTemplateMetaData.Builder templateBuilder = getIndexTemplateMetaData(templateName);
        IndexMetaData.Builder indexMeta = getIndexMetadata(indexName, buildMappingFrom, format);

        MetaData.Builder metaDataBuilder = new MetaData.Builder();
        metaDataBuilder.put(templateBuilder);
        metaDataBuilder.put(indexMeta);

        return ClusterState.builder(state()).metaData(metaDataBuilder.build());
    }

    private void markShardsAvailable(ClusterState.Builder clusterStateBuilder) {
        clusterStateBuilder.routingTable(SecurityTestUtils.buildIndexRoutingTable(INDEX_NAME));
    }

    private static ClusterState state() {
        final DiscoveryNodes nodes = DiscoveryNodes.builder().masterNodeId("1").localNodeId("1").build();
        return ClusterState.builder(CLUSTER_NAME)
                .nodes(nodes)
                .metaData(MetaData.builder().generateClusterUuidIfNeeded())
                .build();
    }

    private static IndexMetaData.Builder getIndexMetadata(String indexName, String templateName, int format) throws IOException {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
        indexMetaData.settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), format)
                .build());

        final Map<String, String> mappings = getTemplateMappings(templateName);
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            indexMetaData.putMapping(entry.getKey(), entry.getValue());
        }

        return indexMetaData;
    }

    private static IndexTemplateMetaData.Builder getIndexTemplateMetaData(String templateName) throws IOException {
        final Map<String, String> mappings = getTemplateMappings(templateName);
        IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(TEMPLATE_NAME)
                .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)));
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            templateBuilder.putMapping(entry.getKey(), entry.getValue());
        }
        return templateBuilder;
    }

    private static Map<String, String> getTemplateMappings(String templateName) {
        String template = loadTemplate(templateName);
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.source(template, XContentType.JSON);
        return request.mappings();
    }

    private static String loadTemplate(String templateName) {
        final String resource = "/" + templateName + ".json";
        return TemplateUtils.loadTemplate(resource, Version.CURRENT.toString(), TEMPLATE_VERSION_PATTERN);
    }

    public void testMappingVersionMatching() throws IOException {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        manager.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertTrue(manager.checkMappingVersion(Version.CURRENT.minimumIndexCompatibilityVersion()::before));
        assertFalse(manager.checkMappingVersion(Version.CURRENT.minimumIndexCompatibilityVersion()::after));
    }

    public void testMissingVersionMappingThrowsError() throws IOException {
        String templateString = "/missing-version-" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();
        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> SecurityIndexManager.checkIndexMappingVersionMatches(SECURITY_INDEX_NAME, clusterState, logger, Version.CURRENT::equals));
        assertEquals("Cannot read security-version string in index " + SECURITY_INDEX_NAME, exception.getMessage());
    }

    public void testIndexTemplateIsIdentifiedAsUpToDate() throws IOException {
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(
            "/" + SECURITY_TEMPLATE_NAME + ".json"
        );
        manager.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        // No upgrade actions run
        assertThat(actions.size(), equalTo(0));
    }

    public void testIndexTemplateVersionMatching() throws Exception {
        String templateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithTemplate(templateString);
        final ClusterState clusterState = clusterStateBuilder.build();

        assertTrue(SecurityIndexManager.checkTemplateExistsAndVersionMatches(
            SecurityIndexManager.SECURITY_TEMPLATE_NAME, clusterState, logger,
            Version.V_6_0_0::before));
        assertFalse(SecurityIndexManager.checkTemplateExistsAndVersionMatches(
            SecurityIndexManager.SECURITY_TEMPLATE_NAME, clusterState, logger,
            Version.V_6_0_0::after));
    }

    public void testUpToDateMappingsAreIdentifiedAsUpToDate() throws IOException {
        String securityTemplateString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        ClusterState.Builder clusterStateBuilder = createClusterStateWithMappingAndTemplate(securityTemplateString);
        manager.clusterChanged(new ClusterChangedEvent("test-event",
            clusterStateBuilder.build(), EMPTY_CLUSTER_STATE));
        assertThat(actions.size(), equalTo(0));
    }

    public void testMissingIndexIsIdentifiedAsUpToDate() throws IOException {
        final ClusterName clusterName = new ClusterName("test-cluster");
        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(clusterName);
        String mappingString = "/" + SECURITY_TEMPLATE_NAME + ".json";
        IndexTemplateMetaData.Builder templateMeta = getIndexTemplateMetaData(SECURITY_TEMPLATE_NAME, mappingString);
        MetaData.Builder builder = new MetaData.Builder(clusterStateBuilder.build().getMetaData());
        builder.put(templateMeta);
        clusterStateBuilder.metaData(builder);
        manager.clusterChanged(new ClusterChangedEvent("test-event", clusterStateBuilder.build()
            , EMPTY_CLUSTER_STATE));
        assertThat(actions.size(), equalTo(0));
    }

    private ClusterState.Builder createClusterStateWithTemplate(String securityTemplateString) throws IOException {
        // add the correct mapping no matter what the template
        ClusterState clusterState = createClusterStateWithIndex("/" + SECURITY_TEMPLATE_NAME + ".json").build();
        final MetaData.Builder metaDataBuilder = new MetaData.Builder(clusterState.metaData());
        metaDataBuilder.put(getIndexTemplateMetaData(SECURITY_TEMPLATE_NAME, securityTemplateString));
        return ClusterState.builder(clusterState).metaData(metaDataBuilder);
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

    private static IndexTemplateMetaData.Builder getIndexTemplateMetaData(String templateName, String templateString) throws IOException {

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
}
