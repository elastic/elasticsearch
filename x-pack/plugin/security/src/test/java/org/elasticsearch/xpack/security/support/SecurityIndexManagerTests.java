/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SecurityIndexManagerTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("security-index-manager-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();
    private SystemIndexDescriptor descriptorSpy;
    private SecurityIndexManager manager;

    @Before
    public void setUpManager() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        // Build a mock client that always accepts put mappings requests
        final Client client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof PutMappingRequest) {
                    listener.onResponse((Response) AcknowledgedResponse.of(true));
                }
            }
        };

        final ClusterService clusterService = mock(ClusterService.class);
        final SystemIndexDescriptor descriptor = new SecuritySystemIndices().getSystemIndexDescriptors()
            .stream()
            .filter(d -> d.getAliasName().equals(SecuritySystemIndices.SECURITY_MAIN_ALIAS))
            .findFirst()
            .get();
        descriptorSpy = spy(descriptor);
        manager = SecurityIndexManager.buildSecurityIndexManager(client, clusterService, descriptorSpy);
    }

    public void testIndexWithUpToDateMappingAndTemplate() {
        assertInitialState();

        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));

        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(true));
        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(true));
    }

    public void testIndexWithoutPrimaryShards() {
        assertInitialState();

        final ClusterState cs = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        ).build();
        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(cs);
        Index index = cs.metadata().index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex();
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        clusterStateBuilder.routingTable(
            RoutingTable.builder()
                .add(
                    IndexRoutingTable.builder(index)
                        .addIndexShard(
                            IndexShardRoutingTable.builder(new ShardId(index, 0))
                                .addShard(
                                    shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize())
                                        .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
                                )
                        )
                        .build()
                )
                .build()
        );
        manager.clusterChanged(event(clusterStateBuilder.build()));

        assertIndexUpToDateButNotAvailable();
    }

    private ClusterChangedEvent event(ClusterState clusterState) {
        return new ClusterChangedEvent("test-event", clusterState, EMPTY_CLUSTER_STATE);
    }

    public void testIndexHealthChangeListeners() {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        final AtomicReference<SecurityIndexManager.State> previousState = new AtomicReference<>();
        final AtomicReference<SecurityIndexManager.State> currentState = new AtomicReference<>();
        final BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> listener = (prevState, state) -> {
            previousState.set(prevState);
            currentState.set(state);
            listenerCalled.set(true);
        };
        manager.addStateListener(listener);

        // index doesn't exist and now exists
        final ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS
        );
        final ClusterState clusterState = markShardsAvailable(clusterStateBuilder);
        manager.clusterChanged(event(clusterState));

        assertTrue(listenerCalled.get());
        assertNull(previousState.get().indexHealth);
        assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexHealth);

        // reset and call with no change to the index
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        ClusterChangedEvent event = new ClusterChangedEvent("same index health", clusterState, clusterState);
        manager.clusterChanged(event);

        assertFalse(listenerCalled.get());
        assertNull(previousState.get());
        assertNull(currentState.get());

        // index with different health
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        Index prevIndex = clusterState.getRoutingTable().index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex();
        final ClusterState newClusterState = ClusterState.builder(clusterState)
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(prevIndex)
                            .addIndexShard(
                                new IndexShardRoutingTable.Builder(new ShardId(prevIndex, 0)).addShard(
                                    ShardRouting.newUnassigned(
                                        new ShardId(prevIndex, 0),
                                        true,
                                        RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                                        ShardRouting.Role.DEFAULT
                                    )
                                        .initialize(UUIDs.randomBase64UUID(random()), null, 0L)
                                        .moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, ""))
                                )
                            )
                    )
                    .build()
            )
            .build();

        event = new ClusterChangedEvent("different index health", newClusterState, clusterState);
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertEquals(ClusterHealthStatus.GREEN, previousState.get().indexHealth);
        assertEquals(ClusterHealthStatus.RED, currentState.get().indexHealth);

        // swap prev and current
        listenerCalled.set(false);
        previousState.set(null);
        currentState.set(null);
        event = new ClusterChangedEvent("different index health swapped", clusterState, newClusterState);
        manager.clusterChanged(event);
        assertTrue(listenerCalled.get());
        assertEquals(ClusterHealthStatus.RED, previousState.get().indexHealth);
        assertEquals(ClusterHealthStatus.GREEN, currentState.get().indexHealth);
    }

    public void testWriteBeforeStateNotRecovered() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);
        manager.prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));
        assertThat(prepareException.get(), is(notNullValue()));
        assertThat(prepareException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) prepareException.get()).status(), is(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(prepareRunnableCalled.get(), is(false));

        prepareException.set(null);
        prepareRunnableCalled.set(false);
        // state not recovered
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).blocks(blocks).build()));
        manager.prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));
        assertThat(prepareException.get(), is(notNullValue()));
        assertThat(prepareException.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) prepareException.get()).status(), is(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(prepareRunnableCalled.get(), is(false));

        prepareException.set(null);
        prepareRunnableCalled.set(false);
        // state recovered with index
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));
        assertThat(prepareException.get(), is(nullValue()));
        assertThat(prepareRunnableCalled.get(), is(true));
    }

    /**
     * Check that the security index manager will update an index's mappings if they are out-of-date.
     * Although the {@index SystemIndexManager} normally handles this, the {@link SecurityIndexManager}
     * expects to be able to handle this also.
     */
    public void testCanUpdateIndexMappings() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);

        // Ensure that the mappings for the index are out-of-date, so that the security index manager will
        // attempt to update them.
        String previousVersion = getPreviousVersion(Version.CURRENT);

        // State recovered with index, with mappings with a prior version
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings(previousVersion)
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));

        manager.prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));

        assertThat(prepareRunnableCalled.get(), is(true));
        assertThat(prepareException.get(), nullValue());
    }

    /**
     * Check that the security index manager will refuse to update mappings on an index
     * if the corresponding {@link SystemIndexDescriptor} requires a higher node version
     * that the cluster's current minimum version.
     */
    public void testCannotUpdateIndexMappingsWhenMinNodeVersionTooLow() {
        final AtomicBoolean prepareRunnableCalled = new AtomicBoolean(false);
        final AtomicReference<Exception> prepareException = new AtomicReference<>(null);

        // Hard-code a failure here.
        doReturn("Nope").when(descriptorSpy).getMinimumNodeVersionMessage(anyString());
        doReturn(null).when(descriptorSpy).getDescriptorCompatibleWith(eq(IndexVersion.CURRENT));

        // Ensure that the mappings for the index are out-of-date, so that the security index manager will
        // attempt to update them.
        String previousVersion = getPreviousVersion(Version.CURRENT);

        // State recovered with index, with mappings with a prior version
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT,
            IndexMetadata.State.OPEN,
            getMappings(previousVersion)
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        manager.prepareIndexIfNeededThenExecute(prepareException::set, () -> prepareRunnableCalled.set(true));

        assertThat(prepareRunnableCalled.get(), is(false));

        final Exception exception = prepareException.get();
        assertThat(exception, not(nullValue()));
        assertThat(exception, instanceOf(IllegalStateException.class));
        assertThat(exception.getMessage(), equalTo("Nope"));
    }

    public void testListenerNotCalledBeforeStateNotRecovered() {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        manager.addStateListener((prev, current) -> listenerCalled.set(true));
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
        // state not recovered
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).blocks(blocks).build()));
        assertThat(manager.isStateRecovered(), is(false));
        assertThat(listenerCalled.get(), is(false));
        // state recovered with index
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertThat(manager.isStateRecovered(), is(true));
        assertThat(listenerCalled.get(), is(true));
    }

    public void testIndexOutOfDateListeners() {
        final AtomicBoolean listenerCalled = new AtomicBoolean(false);
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).build()));
        AtomicBoolean upToDateChanged = new AtomicBoolean();
        manager.addStateListener((prev, current) -> {
            listenerCalled.set(true);
            upToDateChanged.set(prev.isIndexUpToDate != current.isIndexUpToDate);
        });
        assertTrue(manager.isIndexUpToDate());

        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).build()));
        assertFalse(listenerCalled.get());
        assertTrue(manager.isIndexUpToDate());

        // index doesn't exist and now exists with wrong format
        ClusterState.Builder clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT - 1
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertTrue(listenerCalled.get());
        assertTrue(upToDateChanged.get());
        assertFalse(manager.isIndexUpToDate());

        listenerCalled.set(false);
        assertFalse(listenerCalled.get());
        manager.clusterChanged(event(new ClusterState.Builder(CLUSTER_NAME).build()));
        assertTrue(listenerCalled.get());
        assertTrue(upToDateChanged.get());
        assertTrue(manager.isIndexUpToDate());

        listenerCalled.set(false);
        // index doesn't exist and now exists with correct format
        clusterStateBuilder = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT
        );
        manager.clusterChanged(event(markShardsAvailable(clusterStateBuilder)));
        assertTrue(listenerCalled.get());
        assertFalse(upToDateChanged.get());
        assertTrue(manager.isIndexUpToDate());
    }

    public void testProcessClosedIndexState() {
        // Index initially exists
        final ClusterState.Builder indexAvailable = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.OPEN
        );
        manager.clusterChanged(event(markShardsAvailable(indexAvailable)));
        assertThat(manager.indexExists(), is(true));
        assertThat(manager.isAvailable(), is(true));

        // Now close it
        ClusterState.Builder indexClosed = createClusterState(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7,
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            IndexMetadata.State.CLOSE
        );
        if (randomBoolean()) {
            // In old/mixed cluster versions closed indices have no routing table
            indexClosed.routingTable(RoutingTable.EMPTY_ROUTING_TABLE);
        } else {
            indexClosed = ClusterState.builder(markShardsAvailable(indexClosed));
        }

        manager.clusterChanged(event(indexClosed.build()));
        assertThat(manager.indexExists(), is(true));
        assertThat(manager.isAvailable(), is(false));
    }

    private void assertInitialState() {
        assertThat(manager.indexExists(), Matchers.equalTo(false));
        assertThat(manager.isAvailable(), Matchers.equalTo(false));
        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(false));
        assertThat(manager.isStateRecovered(), Matchers.equalTo(false));
    }

    private void assertIndexUpToDateButNotAvailable() {
        assertThat(manager.indexExists(), Matchers.equalTo(true));
        assertThat(manager.isAvailable(), Matchers.equalTo(false));
        assertThat(manager.isMappingUpToDate(), Matchers.equalTo(true));
        assertThat(manager.isStateRecovered(), Matchers.equalTo(true));
    }

    public static ClusterState.Builder createClusterState(String indexName, String aliasName) {
        return createClusterState(indexName, aliasName, IndexMetadata.State.OPEN);
    }

    public static ClusterState.Builder createClusterState(String indexName, String aliasName, IndexMetadata.State state) {
        return createClusterState(indexName, aliasName, SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT, state, getMappings());
    }

    public static ClusterState.Builder createClusterState(String indexName, String aliasName, int format) {
        return createClusterState(indexName, aliasName, format, IndexMetadata.State.OPEN, getMappings());
    }

    private static ClusterState.Builder createClusterState(
        String indexName,
        String aliasName,
        int format,
        IndexMetadata.State state,
        String mappings
    ) {
        IndexMetadata.Builder indexMeta = getIndexMetadata(indexName, aliasName, format, state, mappings);

        Metadata.Builder metadataBuilder = new Metadata.Builder();
        metadataBuilder.put(indexMeta);

        return ClusterState.builder(state()).metadata(metadataBuilder.build());
    }

    private ClusterState markShardsAvailable(ClusterState.Builder clusterStateBuilder) {
        final ClusterState cs = clusterStateBuilder.build();
        return ClusterState.builder(cs)
            .routingTable(
                SecurityTestUtils.buildIndexRoutingTable(
                    cs.metadata().index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7).getIndex()
                )
            )
            .build();
    }

    private static ClusterState state() {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build())
            .masterNodeId("1")
            .localNodeId("1")
            .build();
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes).metadata(Metadata.builder().generateClusterUuidIfNeeded()).build();
    }

    private static IndexMetadata.Builder getIndexMetadata(
        String indexName,
        String aliasName,
        int format,
        IndexMetadata.State state,
        String mappings
    ) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(indexSettings(Version.CURRENT, 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), format));
        indexMetadata.putAlias(AliasMetadata.builder(aliasName).build());
        indexMetadata.state(state);
        if (mappings != null) {
            indexMetadata.putMapping(mappings);
        }

        return indexMetadata;
    }

    private static String getMappings() {
        return getMappings(Version.CURRENT.toString());
    }

    private static String getMappings(String version) {
        try {
            final XContentBuilder builder = jsonBuilder();

            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("security-version", version);
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject("completed");
                    builder.field("type", "boolean");
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build index mappings", e);
        }
    }

    private String getPreviousVersion(Version version) {
        if (version.minor == 0) {
            return version.major - 1 + ".99.0";
        }

        return version.major + "." + (version.minor - 1) + ".0";
    }
}
