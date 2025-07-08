/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsTests;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction.buildStats;
import static org.elasticsearch.action.admin.indices.rollover.TransportRolloverAction.evaluateConditions;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportRolloverActionTests extends ESTestCase {

    final ClusterService mockClusterService = mock(ClusterService.class);
    final DiscoveryNode mockNode = mock(DiscoveryNode.class);
    final ThreadPool mockThreadPool = mock(ThreadPool.class);
    final MetadataCreateIndexService mockCreateIndexService = mock(MetadataCreateIndexService.class);
    final IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
    final ActionFilters mockActionFilters = mock(ActionFilters.class);
    final MetadataIndexAliasesService mdIndexAliasesService = mock(MetadataIndexAliasesService.class);
    final MetadataDataStreamsService mockMetadataDataStreamService = mock(MetadataDataStreamsService.class);
    final Client mockClient = mock(Client.class);
    final AllocationService mockAllocationService = mock(AllocationService.class);
    final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
    final MetadataRolloverService rolloverService = new MetadataRolloverService(
        mockThreadPool,
        mockCreateIndexService,
        mdIndexAliasesService,
        EmptySystemIndices.INSTANCE,
        WriteLoadForecaster.DEFAULT,
        mockClusterService,
        telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
    );
    final DataStreamAutoShardingService dataStreamAutoShardingService = new DataStreamAutoShardingService(
        Settings.EMPTY,
        mockClusterService,
        System::currentTimeMillis
    );

    @Before
    public void setUpMocks() {
        when(mockNode.getId()).thenReturn("mocknode");
        when(mockClusterService.localNode()).thenReturn(mockNode);
    }

    public void testDocStatsSelectionFromPrimariesOnly() {
        long docsInPrimaryShards = 100;
        long docsInShards = 200;

        final Condition<?> condition = createTestCondition();
        String indexName = randomAlphaOfLengthBetween(5, 7);
        evaluateConditions(
            Set.of(condition),
            buildStats(createMetadata(indexName), createIndicesStatResponse(indexName, docsInShards, docsInPrimaryShards))
        );
        final ArgumentCaptor<Condition.Stats> argument = ArgumentCaptor.forClass(Condition.Stats.class);
        verify(condition).evaluate(argument.capture());

        assertEquals(docsInPrimaryShards, argument.getValue().numDocs());
    }

    public void testEvaluateConditions() {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 100)));
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(
            ByteSizeValue.ofMb(randomIntBetween(10, 100))
        );
        MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(10L);
        MinAgeCondition minAgeCondition = new MinAgeCondition(TimeValue.timeValueHours(2));
        MinDocsCondition minDocsCondition = new MinDocsCondition(100L);
        MinSizeCondition minSizeCondition = new MinSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 100)));
        MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = new MinPrimaryShardSizeCondition(
            ByteSizeValue.ofMb(randomIntBetween(10, 100))
        );
        MinPrimaryShardDocsCondition minPrimaryShardDocsCondition = new MinPrimaryShardDocsCondition(10L);
        final Set<Condition<?>> conditions = Set.of(
            maxAgeCondition,
            maxDocsCondition,
            maxSizeCondition,
            maxPrimaryShardSizeCondition,
            maxPrimaryShardDocsCondition,
            minAgeCondition,
            minDocsCondition,
            minSizeCondition,
            minPrimaryShardSizeCondition,
            minPrimaryShardDocsCondition
        );

        long matchMaxDocs = randomIntBetween(100, 1000);
        long notMatchMaxDocs = randomIntBetween(0, 99);
        ByteSizeValue notMatchMaxSize = ByteSizeValue.ofMb(randomIntBetween(0, 9));
        long indexCreated = TimeValue.timeValueHours(3).getMillis();
        long matchMaxPrimaryShardDocs = randomIntBetween(10, 100);
        long notMatchMaxPrimaryShardDocs = randomIntBetween(0, 9);

        expectThrows(
            NullPointerException.class,
            () -> evaluateConditions(null, new Condition.Stats(0, 0, ByteSizeValue.ofMb(0), ByteSizeValue.ofMb(0), 0))
        );

        Map<String, Boolean> results = evaluateConditions(conditions, null);
        assertThat(results.size(), equalTo(10));
        for (Boolean matched : results.values()) {
            assertThat(matched, equalTo(false));
        }

        results = evaluateConditions(
            conditions,
            new Condition.Stats(matchMaxDocs, indexCreated, ByteSizeValue.ofMb(120), ByteSizeValue.ofMb(120), matchMaxPrimaryShardDocs)
        );
        assertThat(results.size(), equalTo(10));
        for (Boolean matched : results.values()) {
            assertThat(matched, equalTo(true));
        }

        results = evaluateConditions(
            conditions,
            new Condition.Stats(notMatchMaxDocs, indexCreated, notMatchMaxSize, ByteSizeValue.ofMb(0), notMatchMaxPrimaryShardDocs)
        );
        assertThat(results.size(), equalTo(10));
        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxPrimaryShardSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxPrimaryShardDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(minDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minPrimaryShardSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minPrimaryShardDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutStats() {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(randomIntBetween(1, 3)));
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomNonNegativeLong());
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong()));
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(
            ByteSizeValue.ofBytes(randomNonNegativeLong())
        );
        MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(randomNonNegativeLong());
        MinAgeCondition minAgeCondition = new MinAgeCondition(TimeValue.timeValueHours(randomIntBetween(1, 3)));
        MinDocsCondition minDocsCondition = new MinDocsCondition(randomNonNegativeLong());
        MinSizeCondition minSizeCondition = new MinSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong()));
        MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = new MinPrimaryShardSizeCondition(
            ByteSizeValue.ofBytes(randomNonNegativeLong())
        );
        MinPrimaryShardDocsCondition minPrimaryShardDocsCondition = new MinPrimaryShardDocsCondition(randomNonNegativeLong());
        final Set<Condition<?>> conditions = Set.of(
            maxAgeCondition,
            maxDocsCondition,
            maxSizeCondition,
            maxPrimaryShardSizeCondition,
            maxPrimaryShardDocsCondition,
            minAgeCondition,
            minDocsCondition,
            minSizeCondition,
            minPrimaryShardSizeCondition,
            minPrimaryShardDocsCondition
        );

        final Settings settings = indexSettings(IndexVersion.current(), randomIntBetween(1, 1000), 10).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        ).build();

        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        Map<String, Boolean> results = evaluateConditions(conditions, buildStats(metadata, null));
        assertThat(results.size(), equalTo(10));

        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxPrimaryShardSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxPrimaryShardDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(minDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minPrimaryShardSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(minPrimaryShardDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutMetadata() {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 100)));
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(
            ByteSizeValue.ofMb(randomIntBetween(10, 100))
        );
        MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(10L);
        MinAgeCondition minAgeCondition = new MinAgeCondition(TimeValue.timeValueHours(2));
        MinDocsCondition minDocsCondition = new MinDocsCondition(100L);
        MinSizeCondition minSizeCondition = new MinSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 100)));
        MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = new MinPrimaryShardSizeCondition(
            ByteSizeValue.ofMb(randomIntBetween(10, 100))
        );
        MinPrimaryShardDocsCondition minPrimaryShardDocsCondition = new MinPrimaryShardDocsCondition(10L);
        final Set<Condition<?>> conditions = Set.of(
            maxAgeCondition,
            maxDocsCondition,
            maxSizeCondition,
            maxPrimaryShardSizeCondition,
            maxPrimaryShardDocsCondition,
            minAgeCondition,
            minDocsCondition,
            minSizeCondition,
            minPrimaryShardSizeCondition,
            minPrimaryShardDocsCondition
        );

        final Settings settings = indexSettings(IndexVersion.current(), randomIntBetween(1, 1000), 10).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        ).build();

        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        IndicesStatsResponse indicesStats = randomIndicesStatsResponse(new IndexMetadata[] { metadata });
        Map<String, Boolean> results = evaluateConditions(conditions, buildStats(null, indicesStats));
        assertThat(results.size(), equalTo(10));
        results.forEach((k, v) -> assertFalse(v));
    }

    public void testConditionEvaluationWhenAliasToWriteAndReadIndicesConsidersOnlyPrimariesFromWriteIndex() throws Exception {
        final Map<String, IndexStats> indexStats = new HashMap<>();
        int total = randomIntBetween(500, 1000);
        indexStats.put("logs-index-000001", createIndexStats(200L, total));
        indexStats.put("logs-index-000002", createIndexStats(300L, total));
        final IndicesStatsResponse statsResponse = createAliasToMultipleIndicesStatsResponse(indexStats);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            @SuppressWarnings("unchecked")
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) args[2];
            listener.onResponse(statsResponse);
            return null;
        }).when(mockClient).execute(eq(IndicesStatsAction.INSTANCE), any(ActionRequest.class), anyActionListener());

        assert statsResponse.getPrimaries().getDocs().getCount() == 500L;
        assert statsResponse.getTotal().getDocs().getCount() == (total + total);

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("logs-index-000001")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(false).build())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("logs-index-000002")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(true).build())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final var projectId = randomProjectIdOrDefault();
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata).put(indexMetadata2))
            .build();

        when(mockCreateIndexService.applyCreateIndexRequest(any(), any(), anyBoolean(), any())).thenReturn(stateBefore);
        when(mdIndexAliasesService.applyAliasActions(any(), any())).thenReturn(stateBefore);

        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );

        // For given alias, verify that condition evaluation fails when the condition doc count is greater than the primaries doc count
        // (primaries from only write index is considered)
        PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
        RolloverRequest rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.setConditions(RolloverConditions.newBuilder().addMaxIndexDocsCondition(500L).build());
        rolloverRequest.dryRun(true);
        transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);

        RolloverResponse response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 500]"), is(false));

        // For given alias, verify that the condition evaluation is successful when condition doc count is less than the primaries doc count
        // (primaries from only write index is considered)
        future = new PlainActionFuture<>();
        rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.setConditions(RolloverConditions.newBuilder().addMaxIndexDocsCondition(300L).build());
        rolloverRequest.dryRun(true);
        transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);

        response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 300]"), is(true));
    }

    public void testLazyRollover() throws Exception {
        final IndexMetadata backingIndexMetadata = IndexMetadata.builder(".ds-logs-ds-000001")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final DataStream dataStream = DataStream.builder("logs-ds", List.of(backingIndexMetadata.getIndex()))
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();
        final var projectId = randomProjectIdOrDefault();
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(backingIndexMetadata, false).put(dataStream))
            .build();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 7;
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) args[6];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(mockMetadataDataStreamService)
            .setRolloverOnWrite(eq(projectId), eq(dataStream.getName()), eq(true), eq(false), any(), any(), anyActionListener());

        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );
        {
            // Regular lazy rollover
            final PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
            RolloverRequest rolloverRequest = new RolloverRequest("logs-ds", null);
            rolloverRequest.lazy(true);
            transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);
            RolloverResponse rolloverResponse = future.actionGet();
            assertThat(rolloverResponse.getOldIndex(), equalTo(".ds-logs-ds-000001"));
            assertThat(rolloverResponse.getNewIndex(), Matchers.startsWith(".ds-logs-ds-"));
            assertThat(rolloverResponse.getNewIndex(), Matchers.endsWith("-000002"));
            assertThat(rolloverResponse.isLazy(), equalTo(true));
            assertThat(rolloverResponse.isDryRun(), equalTo(false));
            assertThat(rolloverResponse.isRolledOver(), equalTo(false));
            assertThat(rolloverResponse.getConditionStatus().size(), equalTo(0));
            assertThat(rolloverResponse.isAcknowledged(), is(true));
        }
        {
            // Dry-run lazy rollover
            final PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
            RolloverRequest rolloverRequest = new RolloverRequest("logs-ds", null);
            rolloverRequest.lazy(true);
            rolloverRequest.dryRun(true);
            transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);
            RolloverResponse rolloverResponse = future.actionGet();
            assertThat(rolloverResponse.getOldIndex(), equalTo(".ds-logs-ds-000001"));
            assertThat(rolloverResponse.getNewIndex(), Matchers.startsWith(".ds-logs-ds-"));
            assertThat(rolloverResponse.getNewIndex(), Matchers.endsWith("-000002"));
            assertThat(rolloverResponse.isLazy(), equalTo(true));
            assertThat(rolloverResponse.isDryRun(), equalTo(true));
            assertThat(rolloverResponse.isRolledOver(), equalTo(false));
            assertThat(rolloverResponse.getConditionStatus().size(), equalTo(0));
            assertThat(rolloverResponse.isAcknowledged(), is(false));
        }
    }

    public void testLazyRolloverFails() throws Exception {
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("logs-index-000001")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(true).build())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final IndexMetadata backingIndexMetadata = IndexMetadata.builder(".ds-logs-ds-000001")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final DataStream dataStream = DataStream.builder("logs-ds", List.of(backingIndexMetadata.getIndex()))
            .setGeneration(randomIntBetween(1, 10))
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();
        final var projectId = randomProjectIdOrDefault();
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata).put(backingIndexMetadata, false).put(dataStream))
            .build();

        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );

        // Lazy rollover fails on a concrete index
        {
            final PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
            RolloverRequest rolloverRequest = new RolloverRequest("logs-alias", null);
            rolloverRequest.lazy(true);
            transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);
            IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class, future::actionGet);
            assertThat(illegalArgumentException.getMessage(), containsString("Lazy rollover can be applied only on a data stream."));
        }

        // Lazy rollover fails when used with conditions
        {
            final PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
            RolloverRequest rolloverRequest = new RolloverRequest("logs-ds", null);
            rolloverRequest.setConditions(RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueDays(1)).build());
            rolloverRequest.lazy(true);
            transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);
            IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class, future::actionGet);
            assertThat(illegalArgumentException.getMessage(), containsString("Lazy rollover can be used only without any conditions."));
        }

        // Lazy rollover fails on concrete index with conditions
        {
            final PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
            RolloverRequest rolloverRequest = new RolloverRequest("logs-alias", null);
            rolloverRequest.setConditions(RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueDays(1)).build());
            rolloverRequest.lazy(true);
            transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);
            IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class, future::actionGet);
            assertThat(
                illegalArgumentException.getMessage(),
                containsString("Lazy rollover can be applied only on a data stream with no conditions.")
            );
        }
    }

    public void testRolloverAliasToDataStreamFails() throws Exception {
        final IndexMetadata backingIndexMetadata = IndexMetadata.builder(".ds-logs-ds-000001")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final DataStream dataStream = DataStream.builder("logs-ds", List.of(backingIndexMetadata.getIndex()))
            .setGeneration(1)
            .setMetadata(Map.of())
            .setIndexMode(IndexMode.STANDARD)
            .build();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(projectId).put(backingIndexMetadata, false).put(dataStream);
        metadataBuilder.put("ds-alias", dataStream.getName(), true, null);
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(metadataBuilder).build();

        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );

        final PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
        RolloverRequest rolloverRequest = new RolloverRequest("ds-alias", null);
        transportRolloverAction.masterOperation(mock(CancellableTask.class), rolloverRequest, stateBefore, future);
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(illegalStateException.getMessage(), containsString("Aliases to data streams cannot be rolled over."));
    }

    public void testCheckBlockForIndices() {
        final var projectId = randomProjectIdOrDefault();
        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );
        final IndexMetadata.Builder indexMetadata1 = IndexMetadata.builder("my-index-1")
            .putAlias(AliasMetadata.builder("my-alias").writeIndex(true).build())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final IndexMetadata indexMetadata2 = IndexMetadata.builder("my-index-2")
            .settings(settings(IndexVersion.current()).put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata1).put(indexMetadata2, false))
            .blocks(ClusterBlocks.builder().addBlocks(projectId, indexMetadata2))
            .build();
        {
            RolloverRequest rolloverRequest = new RolloverRequest("my-alias", "my-new-index");
            when(mockIndexNameExpressionResolver.concreteIndexNames(any(ProjectMetadata.class), any(), (IndicesRequest) any())).thenReturn(
                new String[] { "my-index-1" }
            );
            assertNull(transportRolloverAction.checkBlock(rolloverRequest, stateBefore));
        }
        {
            RolloverRequest rolloverRequest = new RolloverRequest("my-index-2", "my-new-index");
            when(mockIndexNameExpressionResolver.concreteIndexNames(any(ProjectMetadata.class), any(), (IndicesRequest) any())).thenReturn(
                new String[] { "my-index-2" }
            );
            assertNotNull(transportRolloverAction.checkBlock(rolloverRequest, stateBefore));
        }
    }

    public void testCheckBlockForDataStreams() {
        final var projectId = randomProjectIdOrDefault();
        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );
        String dataStreamName = randomAlphaOfLength(20);
        {
            // First, make sure checkBlock returns null when there are no blocks
            final ClusterState clusterState = createDataStream(
                projectId,
                dataStreamName,
                false,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName, null);
            assertNull(transportRolloverAction.checkBlock(rolloverRequest, clusterState));
        }
        {
            // Make sure checkBlock returns null when indices other than the write index have blocks
            final ClusterState clusterState = createDataStream(
                projectId,
                dataStreamName,
                false,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName, null);
            assertNull(transportRolloverAction.checkBlock(rolloverRequest, clusterState));
        }
        {
            // Make sure checkBlock returns null when indices other than the write index have blocks and we use "::data"
            final ClusterState clusterState = createDataStream(
                projectId,
                dataStreamName,
                false,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName + "::data", null);
            assertNull(transportRolloverAction.checkBlock(rolloverRequest, clusterState));
        }
        {
            // Make sure checkBlock returns an exception when the write index has a block
            ClusterState clusterState = createDataStream(
                projectId,
                dataStreamName,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName, null);
            if (randomBoolean()) {
                rolloverRequest.setIndicesOptions(IndicesOptions.lenientExpandOpenNoSelectors());
            }
            ClusterBlockException e = transportRolloverAction.checkBlock(rolloverRequest, clusterState);
            assertNotNull(e);
        }
        {
            // Make sure checkBlock returns an exception when the write index has a block and we use "::data"
            ClusterState clusterState = createDataStream(
                projectId,
                dataStreamName,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName + "::data", null);
            ClusterBlockException e = transportRolloverAction.checkBlock(rolloverRequest, clusterState);
            assertNotNull(e);
        }
    }

    public void testCheckBlockForDataStreamFailureStores() {
        final var projectId = randomProjectIdOrDefault();
        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mock(TransportService.class),
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            TestProjectResolvers.singleProject(projectId),
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient,
            mockAllocationService,
            mockMetadataDataStreamService,
            dataStreamAutoShardingService
        );
        String dataStreamName = randomAlphaOfLength(20);
        {
            // Make sure checkBlock returns no exception when there is no failure store block
            ClusterState clusterState = createDataStream(projectId, dataStreamName, randomBoolean(), randomBoolean(), true, false, false);
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName + "::failures", null);
            assertNull(transportRolloverAction.checkBlock(rolloverRequest, clusterState));
        }
        {
            // Make sure checkBlock returns an exception when the failure store write index has a block
            ClusterState clusterState = createDataStream(
                projectId,
                dataStreamName,
                randomBoolean(),
                randomBoolean(),
                true,
                true,
                randomBoolean()
            );
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName + "::failures", null);
            assertNotNull(transportRolloverAction.checkBlock(rolloverRequest, clusterState));
        }
        {
            // Make sure checkBlock returns no exception when failure store non-write indices have a block
            ClusterState clusterState = createDataStream(projectId, dataStreamName, randomBoolean(), randomBoolean(), true, false, true);
            RolloverRequest rolloverRequest = new RolloverRequest(dataStreamName + "::failures", null);
            assertNull(transportRolloverAction.checkBlock(rolloverRequest, clusterState));
        }
    }

    private ClusterState createDataStream(
        ProjectId projectId,
        String dataStreamName,
        boolean blockOnWriteIndex,
        boolean blocksOnNonWriteIndices,
        boolean includeFailureStore,
        boolean blockOnFailureStoreWriteIndex,
        boolean blockOnFailureStoreNonWriteIndices
    ) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder();
        List<Index> indices = new ArrayList<>();
        int totalIndices = randomIntBetween(1, 20);
        for (int i = 0; i < totalIndices; i++) {
            Settings.Builder settingsBuilder = settings(IndexVersion.current());
            if ((blockOnWriteIndex && i == totalIndices - 1) || (blocksOnNonWriteIndices && i != totalIndices - 1)) {
                settingsBuilder.put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true);
            }
            final IndexMetadata backingIndexMetadata = IndexMetadata.builder(".ds-logs-ds-00000" + (i + 1))
                .settings(settingsBuilder)
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            projectBuilder.put(backingIndexMetadata, false);
            indices.add(backingIndexMetadata.getIndex());
            clusterBlocksBuilder.addBlocks(projectId, backingIndexMetadata);
        }

        DataStream.Builder dataStreamBuilder = DataStream.builder(dataStreamName, indices)
            .setMetadata(Map.of())
            .setIndexMode(randomFrom(IndexMode.values()));
        if (includeFailureStore) {
            List<Index> failureStoreIndices = new ArrayList<>();
            int totalFailureStoreIndices = randomIntBetween(1, 20);
            for (int i = 0; i < totalFailureStoreIndices; i++) {
                Settings.Builder settingsBuilder = settings(IndexVersion.current());
                if ((blockOnFailureStoreWriteIndex && i == totalFailureStoreIndices - 1)
                    || (blockOnFailureStoreNonWriteIndices && i != totalFailureStoreIndices - 1)) {
                    settingsBuilder.put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true);
                }
                final IndexMetadata failureStoreIndexMetadata = IndexMetadata.builder(
                    DataStream.getDefaultFailureStoreName(dataStreamName, i + 1, randomMillisUpToYear9999())
                ).settings(settingsBuilder).numberOfShards(1).numberOfReplicas(1).build();
                failureStoreIndices.add(failureStoreIndexMetadata.getIndex());
                clusterBlocksBuilder.addBlocks(projectId, failureStoreIndexMetadata);
            }
            dataStreamBuilder.setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(failureStoreIndices).build());
        }
        clusterStateBuilder.blocks(clusterBlocksBuilder);
        final DataStream dataStream = dataStreamBuilder.build();
        projectBuilder.put(dataStream);
        return clusterStateBuilder.putProjectMetadata(projectBuilder).build();
    }

    private IndicesStatsResponse createIndicesStatResponse(String indexName, long totalDocs, long primariesDocs) {
        final CommonStats primaryStats = mock(CommonStats.class);
        when(primaryStats.getDocs()).thenReturn(new DocsStats(primariesDocs, 0, between(1, 10000)));

        final CommonStats totalStats = mock(CommonStats.class);
        when(totalStats.getDocs()).thenReturn(new DocsStats(totalDocs, 0, between(1, 10000)));

        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        when(response.getPrimaries()).thenReturn(primaryStats);
        when(response.getTotal()).thenReturn(totalStats);
        final IndexStats indexStats = mock(IndexStats.class);
        when(response.getIndex(indexName)).thenReturn(indexStats);
        when(indexStats.getPrimaries()).thenReturn(primaryStats);
        when(indexStats.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndicesStatsResponse createAliasToMultipleIndicesStatsResponse(Map<String, IndexStats> indexStats) {
        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        final CommonStats primariesStats = new CommonStats();
        final CommonStats totalStats = new CommonStats();
        for (String indexName : indexStats.keySet()) {
            when(response.getIndex(indexName)).thenReturn(indexStats.get(indexName));
            primariesStats.add(indexStats.get(indexName).getPrimaries());
            totalStats.add(indexStats.get(indexName).getTotal());
        }

        when(response.getPrimaries()).thenReturn(primariesStats);
        when(response.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndexStats createIndexStats(long primaries, long total) {
        final CommonStats primariesCommonStats = mock(CommonStats.class);
        when(primariesCommonStats.getDocs()).thenReturn(new DocsStats(primaries, 0, between(1, 10000)));

        final CommonStats totalCommonStats = mock(CommonStats.class);
        when(totalCommonStats.getDocs()).thenReturn(new DocsStats(total, 0, between(1, 10000)));

        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getPrimaries()).thenReturn(primariesCommonStats);
        when(indexStats.getTotal()).thenReturn(totalCommonStats);
        return indexStats;
    }

    private static IndexMetadata createMetadata(String indexName) {
        final Settings settings = indexSettings(IndexVersion.current(), 1, 0).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        ).build();
        return IndexMetadata.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }

    private static Condition<?> createTestCondition() {
        final Condition<?> condition = mock(Condition.class);
        when(condition.evaluate(any())).thenReturn(new Condition.Result(condition, true));
        return condition;
    }

    public static IndicesStatsResponse randomIndicesStatsResponse(final IndexMetadata[] indices) {
        List<ShardStats> shardStats = new ArrayList<>();
        for (final IndexMetadata index : indices) {
            int numShards = randomIntBetween(1, 3);
            int primaryIdx = randomIntBetween(-1, numShards - 1); // -1 means there is no primary shard.
            for (int i = 0; i < numShards; i++) {
                ShardId shardId = new ShardId(index.getIndex(), i);
                boolean primary = (i == primaryIdx);
                Path path = createTempDir().resolve("indices").resolve(index.getIndexUUID()).resolve(String.valueOf(i));
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    primary,
                    primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                    ShardRouting.Role.DEFAULT
                );
                shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats();
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();
                stats.denseVectorStats = new DenseVectorStats();
                stats.sparseVectorStats = new SparseVectorStats();
                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null, false, 0));
            }
        }
        return IndicesStatsTests.newIndicesStatsResponse(
            shardStats.toArray(new ShardStats[shardStats.size()]),
            shardStats.size(),
            shardStats.size(),
            0,
            emptyList(),
            ClusterState.EMPTY_STATE
        );
    }
}
