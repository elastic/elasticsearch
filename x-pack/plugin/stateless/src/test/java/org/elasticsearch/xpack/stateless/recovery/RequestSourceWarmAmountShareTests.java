/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.SourceWarmAmountShare;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.WarmingRatioProviderFactory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportGetShardWarmAmountShareAction;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class RequestSourceWarmAmountShareTests extends ESTestCase {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";

    private final ShardId shardId = new ShardId(new Index("idx", randomUUID()), 0);
    private final long startedAtMillis = randomNonNegativeLong();
    private final Client client = mock(Client.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final Settings.Builder settings = Settings.builder()
        .put(SearchRecoveryTimeoutCalculationService.SEARCH_OFFLINE_WARMING_WARM_AMOUNTS_ENABLED_SETTING.getKey(), true);
    private volatile ClusterState state;

    {
        when(clusterService.state()).thenAnswer(invocation -> state);
    }

    public void testRequestsShareFromShuttingDownSourceForEveryRemovalType() {
        for (var shutdownType : SingleNodeShutdownMetadata.Type.values()) {
            if (shutdownType.isRemovalType() == false) {
                continue;
            }
            clearInvocations(client);
            state = targetState(shutdownType, TransportVersion.current(), true);
            final var share = newListener().requestSourceWarmAmountShare(relocationTarget());

            assertThat(shutdownType.toString(), share.isRequested(), is(true));
            assertThat(share.shutdownSignalTimestamp(), equalTo(startedAtMillis));
            assertThat(share.share(), nullValue());

            final var sent = sentRequest();
            assertThat(sent.request().sourceNode(), sameInstance(state.nodes().get(SOURCE)));
            assertThat(
                sent.request().nodeRequest(),
                equalTo(new TransportGetShardWarmAmountShareAction.NodeRequest(TARGET, shardId, startedAtMillis))
            );

            sent.listener().onResponse(new TransportGetShardWarmAmountShareAction.Response(600L, 1000L));
            assertThat(share.share(), equalTo(new SourceWarmAmountShare.Share(600L, 1000L)));
        }
    }

    public void testFailedRequestLeavesNoShare() {
        state = targetState(randomRemovalType(), TransportVersion.current(), true);
        final var share = newListener().requestSourceWarmAmountShare(relocationTarget());

        final DiscoveryNode sourceNode = state.nodes().get(SOURCE);
        sentRequest().listener()
            .onFailure(
                randomFrom(
                    new ConnectTransportException(sourceNode, "source left"),
                    new NodeClosedException(sourceNode),
                    new ElasticsearchException("simulated")
                )
            );
        assertThat(share.share(), equalTo(SourceWarmAmountShare.Share.NONE));
    }

    public void testNoRequestWhenWarmAmountsDisabled() {
        settings.put(SearchRecoveryTimeoutCalculationService.SEARCH_OFFLINE_WARMING_WARM_AMOUNTS_ENABLED_SETTING.getKey(), false);
        state = targetState(randomRemovalType(), TransportVersion.current(), true);
        assertNotRequested(relocationTarget());
    }

    public void testNoRequestWithoutReplicatedContent() {
        settings.put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), false);
        state = targetState(randomRemovalType(), TransportVersion.current(), true);
        assertNotRequested(relocationTarget());
    }

    public void testNoRequestWhenNotARelocation() {
        state = targetState(randomRemovalType(), TransportVersion.current(), true);
        final ShardRouting routing = randomBoolean()
            ? TestShardRouting.shardRoutingBuilder(shardId, TARGET, false, INITIALIZING).withRole(ShardRouting.Role.SEARCH_ONLY).build()
            : TestShardRouting.shardRoutingBuilder(shardId, TARGET, false, STARTED).withRole(ShardRouting.Role.SEARCH_ONLY).build();
        assertNotRequested(indexShard(routing));
    }

    public void testNoRequestWhenSourceNotShuttingDown() {
        state = targetState(randomFrom(SingleNodeShutdownMetadata.Type.RESTART, null), TransportVersion.current(), true);
        assertNotRequested(relocationTarget());
    }

    public void testNoRequestWhenSourceLeftCluster() {
        state = targetState(randomRemovalType(), TransportVersion.current(), false);
        assertNotRequested(relocationTarget());
    }

    public void testNoRequestWhenClusterDoesNotSupportAction() {
        state = targetState(
            randomRemovalType(),
            TransportVersionUtils.randomVersionNotSupporting(TransportGetShardWarmAmountShareAction.SHARD_WARM_AMOUNT_SHARE),
            true
        );
        assertNotRequested(relocationTarget());
    }

    private static SingleNodeShutdownMetadata.Type randomRemovalType() {
        return randomValueOtherThan(SingleNodeShutdownMetadata.Type.RESTART, () -> randomFrom(SingleNodeShutdownMetadata.Type.values()));
    }

    private void assertNotRequested(IndexShard indexShard) {
        assertThat(newListener().requestSourceWarmAmountShare(indexShard), sameInstance(SourceWarmAmountShare.NOT_REQUESTED));
        verifyNoInteractions(client);
    }

    private StatelessSearchNodeRecoveryListener newListener() {
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings.build(),
                Set.of(
                    SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT,
                    StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING,
                    SearchRecoveryTimeoutCalculationService.SEARCH_OFFLINE_WARMING_WARM_AMOUNTS_ENABLED_SETTING
                )
            )
        );
        return new StatelessSearchNodeRecoveryListener(
            mock(ObjectStoreService.class),
            mock(RecoveryCommitRegistrationHandler.class),
            mock(SharedBlobCacheWarmingService.class),
            DefaultProjectResolver.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            clusterService,
            mock(WarmingRatioProviderFactory.class),
            client
        );
    }

    private record SentRequest(
        TransportGetShardWarmAmountShareAction.Request request,
        ActionListener<TransportGetShardWarmAmountShareAction.Response> listener
    ) {}

    @SuppressWarnings("unchecked")
    private SentRequest sentRequest() {
        final var request = ArgumentCaptor.forClass(TransportGetShardWarmAmountShareAction.Request.class);
        final ArgumentCaptor<ActionListener<TransportGetShardWarmAmountShareAction.Response>> listener = ArgumentCaptor.forClass(
            ActionListener.class
        );
        verify(client).execute(eq(TransportGetShardWarmAmountShareAction.TYPE), request.capture(), listener.capture());
        return new SentRequest(request.getValue(), listener.getValue());
    }

    private IndexShard relocationTarget() {
        return indexShard(
            TestShardRouting.shardRoutingBuilder(shardId, TARGET, false, INITIALIZING)
                .withRelocatingNodeId(SOURCE)
                .withRole(ShardRouting.Role.SEARCH_ONLY)
                .build()
        );
    }

    private IndexShard indexShard(ShardRouting routing) {
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.shardId()).thenReturn(shardId);
        return indexShard;
    }

    /// The state as seen by the target, with the source's shutdown of type `shutdownType` (none if `null`), every node on
    /// `transportVersion`, and the source still in the cluster if `sourcePresent`.
    private ClusterState targetState(
        @Nullable SingleNodeShutdownMetadata.Type shutdownType,
        TransportVersion transportVersion,
        boolean sourcePresent
    ) {
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(TARGET))
            .localNodeId(TARGET)
            .masterNodeId(TARGET);
        final ClusterState.Builder builder = ClusterState.builder(new ClusterName("test"))
            .putCompatibilityVersions(TARGET, transportVersion, Map.of());
        if (sourcePresent) {
            nodes.add(DiscoveryNodeUtils.create(SOURCE));
            builder.putCompatibilityVersions(SOURCE, transportVersion, Map.of());
        }
        final Metadata.Builder metadata = Metadata.builder();
        if (shutdownType != null) {
            final var shutdown = SingleNodeShutdownMetadata.builder()
                .setNodeId(SOURCE)
                .setType(shutdownType)
                .setReason(getTestName())
                .setStartedAtMillis(startedAtMillis)
                .setNodeSeen(true);
            switch (shutdownType) {
                case SIGTERM -> shutdown.setGracePeriod(randomTimeValue());
                case REPLACE -> shutdown.setTargetNodeName("replacement");
                case REMOVE, RESTART -> {
                }
            }
            metadata.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(SOURCE, shutdown.build())));
        }
        return builder.nodes(nodes).metadata(metadata).build();
    }
}
