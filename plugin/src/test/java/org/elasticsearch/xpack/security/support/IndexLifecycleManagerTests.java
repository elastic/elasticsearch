/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import java.io.IOException;
import java.util.Collections;
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
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE;
import static org.elasticsearch.xpack.security.support.IndexLifecycleManager.NULL_MIGRATOR;
import static org.elasticsearch.xpack.security.support.IndexLifecycleManager.TEMPLATE_VERSION_PATTERN;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexLifecycleManagerTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("index-lifecycle-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();
    public static final String INDEX_NAME = "IndexLifecycleManagerTests";
    private static final String TEMPLATE_NAME = "IndexLifecycleManagerTests-template";
    private IndexLifecycleManager manager;
    private IndexLifecycleManager.IndexDataMigrator migrator;
    private Map<Action<?, ?, ?>, Map<ActionRequest, ActionListener<?>>> actions;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUpManager() {
        final Client mockClient = mock(Client.class);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        clusterService = mock(ClusterService.class);

        actions = new LinkedHashMap<>();
        final InternalClient client = new InternalClient(Settings.EMPTY, threadPool, mockClient) {
            @Override
            protected <Request extends ActionRequest,
                    Response extends ActionResponse,
                    RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
            void doExecute(Action<Request, Response, RequestBuilder> action, Request request,
                           ActionListener<Response> listener) {
                final Map<ActionRequest, ActionListener<?>> map = actions.getOrDefault(action, new HashMap<>());
                map.put(request, listener);
                actions.put(action, map);
            }
        };
        migrator = NULL_MIGRATOR;
        manager = new IndexLifecycleManager(Settings.EMPTY, client, clusterService, threadPool, INDEX_NAME, TEMPLATE_NAME,
                // Wrap the migrator in a lambda so that individual tests can override the migrator implementation.
                (previousVersion, listener) -> migrator.performUpgrade(previousVersion, listener)
        );
    }

    public void testIndexWithUpToDateMappingAndTemplate() throws IOException {
        assertInitialState();

        final ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertCompleteState(false);
    }

    public void testIndexWithoutPrimaryShards() throws IOException {
        assertInitialState();

        final ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        Index index = new Index(INDEX_NAME, UUID.randomUUID().toString());
        ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId(index, 0), true, EXISTING_STORE_INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
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

    public void testIndexLifecycleWithOldMappingVersion() throws IOException {
        assertInitialState();

        AtomicReference<ActionListener<Boolean>> migrationListenerRef = new AtomicReference<>(null);
        migrator = (version, listener) -> migrationListenerRef.set(listener);

        ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME + "-v512");
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertTemplateAndMappingOutOfDate(true, false, IndexLifecycleManager.UpgradeState.IN_PROGRESS);

        actions.get(PutIndexTemplateAction.INSTANCE).values().forEach(
                l -> ((ActionListener<PutIndexTemplateResponse>) l).onResponse(new PutIndexTemplateResponse(true) {
                })
        );

        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.IN_PROGRESS);

        migrationListenerRef.get().onResponse(true);

        assertTemplateAndMappingOutOfDate(false, true, IndexLifecycleManager.UpgradeState.COMPLETE);

        actions.get(PutMappingAction.INSTANCE).values().forEach(
                l -> ((ActionListener<PutMappingResponse>) l).onResponse(new PutMappingResponse(true) {
                })
        );

        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.COMPLETE);

        clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertCompleteState(true);
    }

    public void testRetryDataMigration() throws IOException {
        assertInitialState();

        AtomicReference<ActionListener<Boolean>> migrationListenerRef = new AtomicReference<>(null);
        migrator = (version, listener) -> migrationListenerRef.set(listener);

        ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME + "-v512");
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertTemplateAndMappingOutOfDate(true, false, IndexLifecycleManager.UpgradeState.IN_PROGRESS);

        actions.get(PutIndexTemplateAction.INSTANCE).values().forEach(
                l -> ((ActionListener<PutIndexTemplateResponse>) l).onResponse(new PutIndexTemplateResponse(true) {
                })
        );
        actions.get(PutIndexTemplateAction.INSTANCE).clear();

        clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME, TEMPLATE_NAME + "-v512");
        markShardsAvailable(clusterStateBuilder);
        when(clusterService.state()).thenReturn(clusterStateBuilder.build());

        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.IN_PROGRESS);

        AtomicReference<Runnable> scheduled = new AtomicReference<>(null);
        when(threadPool.schedule(any(TimeValue.class), Mockito.eq(ThreadPool.Names.SAME), any(Runnable.class))).thenAnswer(invocation -> {
            final Runnable runnable = (Runnable) invocation.getArguments()[2];
            scheduled.set(runnable);
            return null;
        });


        migrationListenerRef.get().onFailure(new RuntimeException("Migration Failed #1"));
        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.FAILED);
        assertThat(scheduled.get(), notNullValue());

        scheduled.get().run();
        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.IN_PROGRESS);
        scheduled.set(null);

        migrationListenerRef.get().onFailure(new RuntimeException("Migration Failed #2"));
        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.FAILED);
        assertThat(scheduled.get(), notNullValue());

        actions.getOrDefault(PutIndexTemplateAction.INSTANCE, Collections.emptyMap()).clear();
        scheduled.get().run();
        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.IN_PROGRESS);
        scheduled.set(null);

        migrationListenerRef.get().onResponse(false);
        assertTemplateAndMappingOutOfDate(false, true, IndexLifecycleManager.UpgradeState.COMPLETE);

        actions.get(PutMappingAction.INSTANCE).values().forEach(
                l -> ((ActionListener<PutMappingResponse>) l).onResponse(new PutMappingResponse(true) {
                })
        );

        assertTemplateAndMappingOutOfDate(false, false, IndexLifecycleManager.UpgradeState.COMPLETE);

        clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertCompleteState(true);
    }

    public void testIndexHealthChangeListeners() throws Exception {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        final AtomicReference<ClusterIndexHealth> previousHealth = new AtomicReference<>();
        final AtomicReference<ClusterIndexHealth> currentHealth = new AtomicReference<>();
        final BiConsumer<ClusterIndexHealth, ClusterIndexHealth> listener = (prevState, state) -> {
            previousHealth.set(prevState);
            currentHealth.set(state);
            listenerCalled.set(true);
        };

        if (randomBoolean()) {
            if (randomBoolean()) {
                manager.addIndexHealthChangeListener(listener);
                manager.addIndexHealthChangeListener((prevState, state) -> {
                    throw new RuntimeException("throw after listener");
                });
            } else {
                manager.addIndexHealthChangeListener((prevState, state) -> {
                    throw new RuntimeException("throw before listener");
                });
                manager.addIndexHealthChangeListener(listener);
            }
        } else {
            manager.addIndexHealthChangeListener(listener);
        }

        // index doesn't exist and now exists
        final ClusterState.Builder clusterStateBuilder = createClusterState(INDEX_NAME, TEMPLATE_NAME);
        markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterStateBuilder));

        assertTrue(listenerCalled.get());
        assertNull(previousHealth.get());
        assertEquals(ClusterHealthStatus.GREEN, currentHealth.get().getStatus());

        // reset and call with no change to the index
        listenerCalled.set(false);
        previousHealth.set(null);
        currentHealth.set(null);
        ClusterChangedEvent event = new ClusterChangedEvent("same index health", clusterStateBuilder.build(), clusterStateBuilder.build());
        manager.clusterChanged(event);

        assertFalse(listenerCalled.get());
        assertNull(previousHealth.get());
        assertNull(currentHealth.get());

        // index with different health
        listenerCalled.set(false);
        previousHealth.set(null);
        currentHealth.set(null);
        ClusterState previousState = clusterStateBuilder.build();
        Index prevIndex = previousState.getRoutingTable().index(INDEX_NAME).getIndex();
        clusterStateBuilder.routingTable(RoutingTable.builder()
                .add(IndexRoutingTable.builder(prevIndex)
                        .addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(prevIndex, 0))
                                .addShard(ShardRouting.newUnassigned(new ShardId(prevIndex, 0), true, EXISTING_STORE_INSTANCE,
                                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""))
                                        .initialize(UUIDs.randomBase64UUID(random()), null, 0L)
                                        .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "")))
                                .build()))
                .build());



        event = new ClusterChangedEvent("different index health", clusterStateBuilder.build(), previousState);
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertEquals(ClusterHealthStatus.GREEN, previousHealth.get().getStatus());
        assertEquals(ClusterHealthStatus.RED, currentHealth.get().getStatus());

        // swap prev and current
        listenerCalled.set(false);
        previousHealth.set(null);
        currentHealth.set(null);
        event = new ClusterChangedEvent("different index health swapped", previousState, clusterStateBuilder.build());
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertEquals(ClusterHealthStatus.RED, previousHealth.get().getStatus());
        assertEquals(ClusterHealthStatus.GREEN, currentHealth.get().getStatus());
    }

    private void assertInitialState() {
        assertThat(manager.indexExists(), Matchers.equalTo(false));
        assertThat(manager.isAvailable(), Matchers.equalTo(false));

        assertThat(manager.isTemplateUpToDate(), Matchers.equalTo(false));
        assertThat(manager.isTemplateCreationPending(), Matchers.equalTo(false));

        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(false));
        assertThat(manager.getMappingVersion(), Matchers.nullValue());
        assertThat(manager.isMappingUpdatePending(), Matchers.equalTo(false));

        assertThat(manager.getMigrationState(), Matchers.equalTo(IndexLifecycleManager.UpgradeState.NOT_STARTED));
        assertThat(manager.isWritable(), Matchers.equalTo(false));
    }

    private void assertIndexUpToDateButNotAvailable() {
        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(false));

        assertThat(manager.isTemplateUpToDate(), Matchers.equalTo(true));
        assertThat(manager.isTemplateCreationPending(), Matchers.equalTo(false));

        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(manager.getMappingVersion(), Matchers.equalTo(Version.CURRENT));
        assertThat(manager.isMappingUpdatePending(), Matchers.equalTo(false));

        assertThat(manager.isWritable(), Matchers.equalTo(true));
        assertThat(manager.getMigrationState(), Matchers.equalTo(IndexLifecycleManager.UpgradeState.NOT_STARTED));
    }

    private void assertTemplateAndMappingOutOfDate(boolean templateUpdatePending, boolean mappingUpdatePending,
                                                   IndexLifecycleManager.UpgradeState migrationState) {
        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(true));

        assertThat(manager.isTemplateUpToDate(), Matchers.equalTo(!templateUpdatePending));
        assertThat(manager.isTemplateCreationPending(), Matchers.equalTo(templateUpdatePending));

        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(false));
        assertThat(manager.getMappingVersion(), Matchers.equalTo(Version.V_5_1_2));
        assertThat(manager.isMappingUpdatePending(), Matchers.equalTo(mappingUpdatePending));

        assertThat(manager.isWritable(), Matchers.equalTo(false));
        assertThat(manager.getMigrationState(), Matchers.equalTo(migrationState));

        if (templateUpdatePending) {
            final Map<ActionRequest, ActionListener<?>> requests = actions.get(PutIndexTemplateAction.INSTANCE);
            assertThat(requests, notNullValue());
            assertThat(requests.size(), Matchers.equalTo(1));
            final ActionRequest request = requests.keySet().iterator().next();
            assertThat(request, Matchers.instanceOf(PutIndexTemplateRequest.class));
            assertThat(((PutIndexTemplateRequest) request).name(), Matchers.equalTo(TEMPLATE_NAME));
        }

        if (mappingUpdatePending) {
            final Map<ActionRequest, ActionListener<?>> requests = actions.get(PutMappingAction.INSTANCE);
            assertThat(requests, notNullValue());
            assertThat(requests.size(), Matchers.equalTo(1));
            final ActionRequest request = requests.keySet().iterator().next();
            assertThat(request, Matchers.instanceOf(PutMappingRequest.class));
            assertThat(((PutMappingRequest) request).indices(), Matchers.arrayContainingInAnyOrder(INDEX_NAME));
            assertThat(((PutMappingRequest) request).type(), Matchers.equalTo("doc"));
        }
    }

    private void assertCompleteState(boolean expectMigration) {
        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(true));

        assertThat(manager.isTemplateUpToDate(), Matchers.equalTo(true));
        assertThat(manager.isTemplateCreationPending(), Matchers.equalTo(false));

        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(manager.getMappingVersion(), Matchers.equalTo(Version.CURRENT));
        assertThat(manager.isMappingUpdatePending(), Matchers.equalTo(false));

        assertThat(manager.isWritable(), Matchers.equalTo(true));
        if (expectMigration) {
            assertThat(manager.getMigrationState(), Matchers.equalTo(IndexLifecycleManager.UpgradeState.COMPLETE));
        } else {
            assertThat(manager.getMigrationState(), Matchers.equalTo(IndexLifecycleManager.UpgradeState.NOT_STARTED));
        }
    }

    public static ClusterState.Builder createClusterState(String indexName, String templateName) throws IOException {
        return createClusterState(indexName, templateName, templateName);
    }

    private static ClusterState.Builder createClusterState(String indexName, String templateName, String buildMappingFrom)
            throws IOException {
        IndexTemplateMetaData.Builder templateBuilder = getIndexTemplateMetaData(templateName);
        IndexMetaData.Builder indexMeta = getIndexMetadata(indexName, buildMappingFrom);

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

    private static IndexMetaData.Builder getIndexMetadata(String indexName, String templateName) throws IOException {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(indexName);
        indexMetaData.settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .build());

        final Map<String, String> mappings = getTemplateMappings(templateName);
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            indexMetaData.putMapping(entry.getKey(), entry.getValue());
        }

        return indexMetaData;
    }

    private static IndexTemplateMetaData.Builder getIndexTemplateMetaData(String templateName) throws IOException {
        final Map<String, String> mappings = getTemplateMappings(templateName);
        IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(TEMPLATE_NAME);
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
}