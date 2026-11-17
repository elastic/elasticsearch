/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetadataMappingService.PreflightCacheEntry;
import org.elasticsearch.cluster.metadata.MetadataMappingService.PutMappingClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;

public class MetadataMappingServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testMappingClusterStateUpdateDoesntChangeExistingIndices() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").setMapping());
        final CompressedXContent currentMapping = indexService.mapperService().documentMapper().mappingSource();

        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(IndexSettingProviders.EMPTY, true);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        // TODO - it will be nice to get a random mapping generator
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        // the task really was a mapping update
        assertThat(
            indexService.mapperService().documentMapper().mappingSource(),
            not(equalTo(resultingState.metadata().getProject().index("test").mapping().source()))
        );
        // since we never committed the cluster state update, the in-memory state is unchanged
        assertThat(indexService.mapperService().documentMapper().mappingSource(), equalTo(currentMapping));
    }

    public void testMappingUpdateInProjectUnderDeletion() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final ProjectId projectId = randomUniqueProjectId();
        final Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexService.getMetadata(), false)).build();
        final ClusterState initialState = ClusterState.builder(getInstanceFromNode(ClusterService.class).state())
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .blocks(ClusterBlocks.builder().addProjectGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK))
            .build();
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );

        final ClusterState resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            initialState,
            newExecutor(IndexSettingProviders.EMPTY, true),
            singleTask(request),
            task -> fail("mapping update should have failed"),
            (task, e) -> {
                assertThat(e, instanceOf(ClusterBlockException.class));
                final ClusterBlockException clusterBlockException = (ClusterBlockException) e;
                assertTrue(clusterBlockException.blocks().contains(ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK));
            }
        );
        assertSame(initialState, resultingState);
    }

    public void testClusterStateIsNotChangedWithIdenticalMappings() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));

        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(IndexSettingProviders.EMPTY, true);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState1 = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        final var resultingState2 = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            resultingState1,
            putMappingExecutor,
            singleTask(request)
        );
        assertSame(resultingState1, resultingState2);
    }

    public void testMappingVersion() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final long previousVersion = indexService.getMetadata().getMappingVersion();
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(IndexSettingProviders.EMPTY, true);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        assertThat(resultingState.metadata().getProject().index("test").getMappingVersion(), equalTo(1 + previousVersion));
        assertThat(resultingState.metadata().getProject().index("test").getMappingsUpdatedVersion(), equalTo(IndexVersion.current()));
    }

    public void testMappingVersionUnchanged() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").setMapping());
        final long previousVersion = indexService.getMetadata().getMappingVersion();
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(IndexSettingProviders.EMPTY, true);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "{ \"properties\": {}}",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        assertThat(resultingState.metadata().getProject().index("test").getMappingVersion(), equalTo(previousVersion));
    }

    public void testUpdateSettings() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final long previousVersion = indexService.getMetadata().getSettingsVersion();
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(
            new IndexSettingProviders(Set.of(new IndexSettingProvider() {
                @Override
                public void provideAdditionalSettings(
                    String indexName,
                    String dataStreamName,
                    IndexMode templateIndexMode,
                    boolean registryInstalledTemplate,
                    ProjectMetadata projectMetadata,
                    Instant resolvedAt,
                    Settings indexTemplateAndCreateRequestSettings,
                    List<CompressedXContent> combinedTemplateMappings,
                    IndexVersion indexVersion,
                    Settings.Builder additionalSettings
                ) {}

                @Override
                public void onUpdateMappings(
                    IndexMetadata indexMetadata,
                    DocumentMapper documentMapper,
                    Settings.Builder additionalSettings
                ) {
                    additionalSettings.put("index.mapping.total_fields.limit", 42);
                }

                @Override
                public Set<String> affectedSettings() {
                    return Set.of("index.mapping.total_fields.limit");
                }
            })),
            true
        );
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );

        IndexMetadata indexMetadata = resultingState.metadata().indexMetadata(indexService.index());
        assertThat(indexMetadata.getSettingsVersion(), equalTo(1 + previousVersion));
        assertThat(indexMetadata.getSettings().get("index.mapping.total_fields.limit"), equalTo("42"));
    }

    /**
     * Test that putting an identical mapping results in a no-op and does not submit a cluster state update task.
     */
    public void testMappingNoOpUpdateExactEquals() throws IOException {
        runNoOpMappingUpdateTest("""
            {"_doc":{"properties":{"field":{"type":"keyword"}}}}""");
    }

    /**
     * Test that putting a mapping that is semantically identical but syntactically different results in a no-op.
     */
    public void testMappingNoOpUpdateSemanticEquals() throws IOException {
        runNoOpMappingUpdateTest("""
            {"properties": {"field": {"type": "keyword", "ignore_above": "2147483647"}}}""");
    }

    @SuppressWarnings("unchecked")
    private void runNoOpMappingUpdateTest(String updatedMapping) throws IOException {
        // Create index with initial mapping
        final var indexService = createIndex("test", Settings.EMPTY, "field", "type=keyword");

        // Set up MetadataMappingService with a mocked ClusterService that prevents and monitors task submission
        final ClusterService clusterService = Mockito.spy(getInstanceFromNode(ClusterService.class));
        final MasterServiceTaskQueue<PutMappingClusterStateUpdateTask> masterServiceTaskQueue = Mockito.mock(MasterServiceTaskQueue.class);
        Mockito.doThrow(new AssertionError("not supposed to run")).when(masterServiceTaskQueue).submitTask(any(), any(), any());
        Mockito.when(clusterService.<PutMappingClusterStateUpdateTask>createTaskQueue(any(), any(), any()))
            .thenReturn(masterServiceTaskQueue);
        final MetadataMappingService metadataMappingService = new MetadataMappingService(
            clusterService,
            getInstanceFromNode(IndicesService.class),
            IndexSettingProviders.EMPTY
        );

        // Put updated mapping
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            updatedMapping,
            randomBoolean(),
            indexService.index()
        );
        metadataMappingService.putMapping(request, future);
        safeGet(future);

        // Verify that no cluster state update task was submitted
        Mockito.verifyNoInteractions(masterServiceTaskQueue);
    }

    /** Cache hit: pre-flight service is reused and mapping version increments. */
    public void testCacheHitAppliesMapping() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(IndexSettingProviders.EMPTY, true);
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        final String newMapping = """
            { "properties": { "field": { "type": "keyword" }}}""";

        final Map<Index, PreflightCacheEntry> preflightCache = new HashMap<>();
        preflightCache.put(index, buildCacheEntry(indicesService, indexMetadata, newMapping));
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            newMapping,
            false,
            index
        );

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            List.of(new PutMappingClusterStateUpdateTask(request, ActionListener.wrap(r -> {}, e -> {}), preflightCache))
        );

        assertThat(resultingState.metadata().indexMetadata(index).getMappingVersion(), equalTo(indexMetadata.getMappingVersion() + 1));
        assertThat(resultingState.metadata().indexMetadata(index).mapping().source().string(), containsString("\"field\""));
        // The success-path finally block in execute() closes and clears the cache map.
        assertTrue(preflightCache.isEmpty());
    }

    /** Stale mapping version: cache entry is discarded and a fresh service is used. */
    public void testCacheMissStaleMappingVersionFallsBack() throws Exception {
        runCacheMissTest(-1, 0);
    }

    /** Stale settings version: cache entry is discarded (settings are baked in at creation time). */
    public void testCacheMissStaleSettingsVersionFallsBack() throws Exception {
        runCacheMissTest(0, -1);
    }

    /** onFailure on a never-executed task closes all cache entries and clears the map. */
    public void testOnFailureClosesAndClearsPreflightCache() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        final String mapping = """
            { "properties": { "field": { "type": "keyword" }}}""";

        final Map<Index, PreflightCacheEntry> preflightCache = new HashMap<>();
        final PreflightCacheEntry entry = buildCacheEntry(indicesService, indexMetadata, mapping);
        preflightCache.put(index, entry);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            mapping,
            false,
            index
        );
        final PutMappingClusterStateUpdateTask task = new PutMappingClusterStateUpdateTask(
            request,
            ActionListener.wrap(r -> fail("should not succeed"), e -> {}),
            preflightCache
        );

        task.onFailure(new RuntimeException("simulated failure"));

        assertTrue("preflightCache should be cleared after onFailure", preflightCache.isEmpty());
        Mockito.verify(entry.mergedService()).close();
    }

    public void testCachedServicesAreClosedAfterMappingUpdates() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final IndexMetadata metadata = initial.metadata().indexMetadata(index);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        for (boolean autoUpdate : List.of(false, true)) {
            final List<PutMappingClusterStateUpdateTask> tasks = new ArrayList<>();
            final List<PreflightCacheEntry> entries = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                final PutMappingClusterStateUpdateRequest request = request(autoUpdate, field("field" + i, "long"), index);
                final PreflightCacheEntry entry = buildCacheEntry(indicesService, metadata, request.source().string());
                entries.add(entry);
                tasks.add(new PutMappingClusterStateUpdateTask(request, ActionListener.noop(), new HashMap<>(Map.of(index, entry))));
            }
            final ClusterState result = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                initial,
                newExecutor(IndexSettingProviders.EMPTY, true),
                tasks
            );
            assertEquals(metadata.getMappingVersion() + (autoUpdate ? 1 : 2), result.metadata().indexMetadata(index).getMappingVersion());
            for (int i = 0; i < tasks.size(); i++) {
                assertThat(fieldTypeName(result, index, "field" + i), equalTo("long"));
                assertTrue(tasks.get(i).preflightCache().isEmpty());
                Mockito.verify(entries.get(i).mergedService()).close();
            }
        }
    }

    public void testFailedGroupClosesCachedServicesDuringFallback() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final IndexMetadata metadata = initial.metadata().indexMetadata(index);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final RecordingIndexSettingProvider provider = new RecordingIndexSettingProvider();
        provider.failOnMapper = mapper -> mapper.mappers().getMapper("rejected") != null;
        final List<PutMappingClusterStateUpdateTask> tasks = new ArrayList<>();
        final List<PreflightCacheEntry> entries = new ArrayList<>();
        for (String name : List.of("rejected", "following")) {
            final PutMappingClusterStateUpdateRequest request = request(true, field(name, "long"), index);
            final PreflightCacheEntry entry = buildCacheEntry(indicesService, metadata, request.source().string());
            entries.add(entry);
            tasks.add(new PutMappingClusterStateUpdateTask(request, ActionListener.noop(), new HashMap<>(Map.of(index, entry))));
        }
        final ClusterState result = ClusterStateTaskExecutorUtils.executeHandlingResults(
            initial,
            newExecutor(new IndexSettingProviders(Set.of(provider)), true),
            tasks,
            task -> assertSame(tasks.getLast(), task),
            (task, failure) -> {
                assertSame(tasks.getFirst(), task);
                assertThat(failure, instanceOf(IllegalStateException.class));
                assertThat(failure.getMessage(), containsString("simulated index setting provider failure"));
            }
        );
        assertNull(fieldTypeName(result, index, "rejected"));
        assertThat(fieldTypeName(result, index, "following"), equalTo("long"));
        for (int i = 0; i < tasks.size(); i++) {
            assertTrue(tasks.get(i).preflightCache().isEmpty());
            Mockito.verify(entries.get(i).mergedService()).close();
        }
    }

    public void testDeletedIndexGroupFailsAndClosesPreflightCaches() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final IndexMetadata metadata = initial.metadata().indexMetadata(index);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final List<PutMappingClusterStateUpdateTask> tasks = new ArrayList<>();
        final List<PreflightCacheEntry> entries = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final PutMappingClusterStateUpdateRequest request = request(true, field("field" + i, "long"), index);
            final PreflightCacheEntry entry = buildCacheEntry(indicesService, metadata, request.source().string());
            entries.add(entry);
            tasks.add(new PutMappingClusterStateUpdateTask(request, ActionListener.noop(), new HashMap<>(Map.of(index, entry))));
        }
        assertAcked(indicesAdmin().prepareDelete(index.getName()));
        final ClusterState deleted = getInstanceFromNode(ClusterService.class).state();
        final ClusterState result = ClusterStateTaskExecutorUtils.executeHandlingResults(
            deleted,
            newExecutor(IndexSettingProviders.EMPTY, true),
            tasks,
            task -> fail("deleted index should fail"),
            (task, failure) -> assertThat(failure, instanceOf(IndexNotFoundException.class))
        );
        assertSame(deleted, result);
        for (int i = 0; i < tasks.size(); i++) {
            assertTrue(tasks.get(i).preflightCache().isEmpty());
            Mockito.verify(entries.get(i).mergedService()).close();
        }
    }

    public void testGroupMergesOnlyRemainingTasksOnFirstTasksPreflightService() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final IndexMetadata metadata = initial.metadata().indexMetadata(index);
        final List<PutMappingClusterStateUpdateRequest> requests = List.of(
            request(true, field("field0", "long"), index),
            request(true, field("field1", "long"), index),
            request(true, field("field2", "long"), index)
        );
        final PreflightCacheEntry entry = buildCacheEntry(
            getInstanceFromNode(IndicesService.class),
            metadata,
            requests.getFirst().source().string()
        );
        final List<PutMappingClusterStateUpdateTask> tasks = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            final Map<Index, PreflightCacheEntry> cache = i == 0 ? new HashMap<>(Map.of(index, entry)) : new HashMap<>();
            tasks.add(new PutMappingClusterStateUpdateTask(requests.get(i), ActionListener.noop(), cache));
        }

        final ClusterState result = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            initial,
            newExecutor(IndexSettingProviders.EMPTY, true),
            tasks
        );

        for (int i = 0; i < requests.size(); i++) {
            assertThat(fieldTypeName(result, index, "field" + i), equalTo("long"));
        }
        assertThat(result.metadata().indexMetadata(index).getMappingVersion(), equalTo(metadata.getMappingVersion() + 1));
        Mockito.verify(entry.mergedService()).mergeDynamicMappingUpdates(List.of(requests.get(1).source(), requests.get(2).source()));
        Mockito.verify(entry.mergedService()).close();
    }

    public void testFailedGroupSeededFromPreflightServiceFallsBackFromFreshService() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final IndexMetadata metadata = initial.metadata().indexMetadata(index);
        final PutMappingClusterStateUpdateRequest cached = request(true, field("cached", "long"), index);
        final PutMappingClusterStateUpdateRequest conflicting = request(
            true,
            "\"existing\":{\"type\":\"object\",\"properties\":{\"sub\":{\"type\":\"long\"}}}",
            index
        );
        final PreflightCacheEntry entry = buildCacheEntry(getInstanceFromNode(IndicesService.class), metadata, cached.source().string());
        final List<PutMappingClusterStateUpdateTask> tasks = List.of(
            new PutMappingClusterStateUpdateTask(cached, ActionListener.noop(), new HashMap<>(Map.of(index, entry))),
            new PutMappingClusterStateUpdateTask(conflicting, ActionListener.noop(), new HashMap<>())
        );

        final ClusterState result = ClusterStateTaskExecutorUtils.executeHandlingResults(
            initial,
            newExecutor(IndexSettingProviders.EMPTY, true),
            tasks,
            task -> assertSame(tasks.getFirst(), task),
            (task, failure) -> assertSame(tasks.getLast(), task)
        );

        assertThat(fieldTypeName(result, index, "cached"), equalTo("long"));
        assertThat(fieldTypeName(result, index, "existing"), equalTo("keyword"));
        assertThat(result.metadata().indexMetadata(index).getMappingVersion(), equalTo(metadata.getMappingVersion() + 1));
        Mockito.verify(entry.mergedService()).close();
    }

    /** Shared body: builds a stale cache entry (versions offset by deltas) and asserts fall-back. */
    private void runCacheMissTest(long mappingVersionDelta, long settingsVersionDelta) throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = newExecutor(IndexSettingProviders.EMPTY, true);
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        // The stale cache entry uses a different field name than the request so we can confirm the
        // stale entry was discarded and the request mapping was applied from a fresh service.
        final String staleMapping = """
            { "properties": { "stale_field": { "type": "keyword" }}}""";
        final String requestMapping = """
            { "properties": { "field": { "type": "keyword" }}}""";

        final Map<Index, PreflightCacheEntry> preflightCache = new HashMap<>();
        preflightCache.put(
            index,
            buildCacheEntry(
                indicesService,
                indexMetadata,
                staleMapping,
                indexMetadata.getMappingVersion() + mappingVersionDelta,
                indexMetadata.getSettingsVersion() + settingsVersionDelta
            )
        );
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            requestMapping,
            false,
            index
        );

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            List.of(new PutMappingClusterStateUpdateTask(request, ActionListener.wrap(r -> {}, e -> {}), preflightCache))
        );

        assertThat(resultingState.metadata().indexMetadata(index).getMappingVersion(), equalTo(indexMetadata.getMappingVersion() + 1));
        final String appliedMapping = resultingState.metadata().indexMetadata(index).mapping().source().string();
        assertThat(appliedMapping, containsString("\"field\""));
        assertThat(appliedMapping, not(containsString("\"stale_field\"")));
        assertTrue(preflightCache.isEmpty());
    }

    /** Mirrors what {@code isWholeRequestNoop} does on the MANAGEMENT thread to build a cache entry. */
    private PreflightCacheEntry buildCacheEntry(IndicesService indicesService, IndexMetadata indexMetadata, String mapping)
        throws IOException {
        return buildCacheEntry(
            indicesService,
            indexMetadata,
            mapping,
            indexMetadata.getMappingVersion(),
            indexMetadata.getSettingsVersion()
        );
    }

    private PreflightCacheEntry buildCacheEntry(
        IndicesService indicesService,
        IndexMetadata indexMetadata,
        String mapping,
        long mappingVersion,
        long settingsVersion
    ) throws IOException {
        // A spy keeps real merging and cleanup while allowing ownership assertions; MapperService exposes no closed-state accessor.
        final MapperService service = Mockito.spy(indicesService.createIndexMapperServiceForValidation(indexMetadata));
        service.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
        final CompressedXContent preUpdateSource = service.documentMapper() != null ? service.documentMapper().mappingSource() : null;
        service.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        return new PreflightCacheEntry(mappingVersion, settingsVersion, preUpdateSource, service);
    }

    private MetadataMappingService.PutMappingExecutor newExecutor(IndexSettingProviders providers, boolean batchingEnabled) {
        return getInstanceFromNode(MetadataMappingService.class).new PutMappingExecutor(providers, () -> batchingEnabled);
    }

    private record ExecutionOutcome(ClusterState state, List<String> taskResults) {}

    private ExecutionOutcome execute(ClusterState state, boolean batchingEnabled, List<PutMappingClusterStateUpdateRequest> requests)
        throws Exception {
        return execute(state, batchingEnabled, IndexSettingProviders.EMPTY, requests);
    }

    private ExecutionOutcome execute(
        ClusterState state,
        boolean batchingEnabled,
        IndexSettingProviders providers,
        List<PutMappingClusterStateUpdateRequest> requests
    ) throws Exception {
        final List<PutMappingClusterStateUpdateTask> tasks = requests.stream()
            .map(r -> new PutMappingClusterStateUpdateTask(r, ActionListener.noop(), new HashMap<>()))
            .toList();
        final String[] results = new String[tasks.size()];
        final ClusterState resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            state,
            newExecutor(providers, batchingEnabled),
            tasks,
            task -> results[identityIndexOf(tasks, task)] = "success",
            (task, e) -> results[identityIndexOf(tasks, task)] = e.getClass().getName() + ": " + e.getMessage()
        );
        return new ExecutionOutcome(resultingState, Arrays.asList(results));
    }

    private static int identityIndexOf(List<PutMappingClusterStateUpdateTask> tasks, PutMappingClusterStateUpdateTask task) {
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i) == task) {
                return i;
            }
        }
        throw new AssertionError("unknown task");
    }

    private static PutMappingClusterStateUpdateRequest request(boolean autoUpdate, String properties, Index... indices) throws IOException {
        return new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "{\"properties\":{" + properties + "}}",
            autoUpdate,
            indices
        );
    }

    private static String field(String name, String type) {
        return "\"" + name + "\":{\"type\":\"" + type + "\"}";
    }

    private Index createIndexWithExistingField(String name) {
        return createIndex(name, client().admin().indices().prepareCreate(name).setMapping("existing", "type=keyword")).index();
    }

    private static Object fieldTypeName(ClusterState state, Index index, String field) {
        return XContentMapValues.extractValue(
            "properties." + field + ".type",
            state.metadata().getProject().index(index).mapping().sourceAsMap()
        );
    }

    private static CompressedXContent mappingSource(ClusterState state, Index index) {
        return state.metadata().getProject().index(index).mapping().source();
    }

    public void testBatchedDynamicMappingUpdatesForOneIndexBumpMappingVersionOnce() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final long previousVersion = initial.metadata().getProject().index(index).getMappingVersion();
        final List<PutMappingClusterStateUpdateRequest> requests = new ArrayList<>();
        final int requestCount = randomIntBetween(2, 10);
        for (int i = 0; i < requestCount; i++) {
            requests.add(request(true, field("field" + i, randomFrom("long", "keyword")), index));
        }

        final RecordingIndexSettingProvider provider = new RecordingIndexSettingProvider();
        final ExecutionOutcome batched = execute(initial, true, new IndexSettingProviders(Set.of(provider)), requests);
        final ExecutionOutcome sequential = execute(initial, false, requests);

        assertThat(batched.taskResults(), equalTo(Collections.nCopies(requests.size(), "success")));
        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
        assertThat(provider.mappers.size(), equalTo(1));
        assertThat(provider.mappers.get(0).mappingSource(), equalTo(mappingSource(batched.state(), index)));
        assertThat(batched.state().metadata().getProject().index(index).getMappingVersion(), equalTo(previousVersion + 1));
        assertThat(sequential.state().metadata().getProject().index(index).getMappingVersion(), equalTo(previousVersion + requests.size()));
    }

    public void testGroupFallsBackWhenIndexSettingProviderFailsAfterMerge() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final RecordingIndexSettingProvider provider = new RecordingIndexSettingProvider();
        provider.failOnMapper = mapper -> mapper.mappers().getMapper("field2") != null && mapper.mappers().getMapper("field3") != null;
        final List<PutMappingClusterStateUpdateRequest> requests = List.of(
            request(true, field("field1", "long"), index),
            request(true, field("field2", "long"), index),
            request(true, field("field3", "long"), index)
        );

        final ExecutionOutcome outcome = execute(initial, true, new IndexSettingProviders(Set.of(provider)), requests);

        assertThat(outcome.taskResults().get(0), equalTo("success"));
        assertThat(outcome.taskResults().get(1), equalTo("success"));
        assertThat(outcome.taskResults().get(2), not(equalTo("success")));
        assertThat(fieldTypeName(outcome.state(), index, "field2"), equalTo("long"));
        assertNull(fieldTypeName(outcome.state(), index, "field3"));
    }

    public void testNonAutoAndMultiIndexTasksAreBarriers() throws Exception {
        final Index index1 = createIndexWithExistingField("test1");
        final Index index2 = createIndexWithExistingField("test2");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();

        // a conflicting explicit update that only fails if the preceding dynamic mapping update has already been applied
        final List<PutMappingClusterStateUpdateRequest> nonAuto = List.of(
            request(true, field("field1", "long"), index1),
            request(true, field("field2", "long"), index1),
            request(false, field("field1", "keyword"), index1),
            request(true, field("field3", "long"), index1)
        );
        ExecutionOutcome batched = execute(initial, true, nonAuto);
        ExecutionOutcome sequential = execute(initial, false, nonAuto);
        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(batched.taskResults().get(2), not(equalTo("success")));
        assertThat(mappingSource(batched.state(), index1), equalTo(mappingSource(sequential.state(), index1)));

        final List<PutMappingClusterStateUpdateRequest> multiIndex = List.of(
            request(true, field("field1", "long"), index2),
            request(true, field("field2", "long"), index2),
            request(false, field("field1", "keyword"), index1, index2),
            request(true, field("field3", "long"), index2)
        );
        batched = execute(initial, true, multiIndex);
        sequential = execute(initial, false, multiIndex);
        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(batched.taskResults().get(2), not(equalTo("success")));
        for (Index index : List.of(index1, index2)) {
            assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
        }
        assertNull(fieldTypeName(batched.state(), index1, "field1"));
        assertThat(batched.taskResults().getLast(), equalTo("success"));
        assertThat(fieldTypeName(batched.state(), index2, "field3"), equalTo("long"));
    }

    public void testFailedFallbackTaskDoesNotAffectFollowingTasks() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final RecordingIndexSettingProvider provider = new RecordingIndexSettingProvider();
        provider.failOnMapper = mapper -> mapper.mappers().getMapper("rejected") != null;
        final IndexSettingProviders providers = new IndexSettingProviders(Set.of(provider));
        for (boolean precedingSuccess : List.of(false, true)) {
            final List<PutMappingClusterStateUpdateRequest> requests = new ArrayList<>();
            if (precedingSuccess) {
                requests.add(request(false, field("accepted", "long"), index));
            }
            requests.add(request(true, field("rejected", "long"), index));
            requests.add(request(true, "\"existing\":{\"type\":\"object\",\"properties\":{\"sub\":{\"type\":\"long\"}}}", index));
            requests.add(request(true, field("following", "long"), index));

            final ExecutionOutcome batched = execute(initial, true, providers, requests);
            final ExecutionOutcome sequential = execute(initial, false, providers, requests);

            assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
            assertThat(batched.taskResults().get(requests.size() - 3), containsString("simulated index setting provider failure"));
            assertThat(batched.taskResults().get(requests.size() - 2), not(equalTo("success")));
            assertThat(batched.taskResults().getLast(), equalTo("success"));
            assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
            assertNull(fieldTypeName(batched.state(), index, "rejected"));
            assertThat(fieldTypeName(batched.state(), index, "existing"), equalTo("keyword"));
            assertThat(fieldTypeName(batched.state(), index, "following"), equalTo("long"));
            if (precedingSuccess) {
                assertThat(fieldTypeName(batched.state(), index, "accepted"), equalTo("long"));
            }
        }
    }

    public void testBatchingSettingIsDynamicAndUpdatesStillApply() throws Exception {
        final String settingKey = MetadataMappingService.PUT_MAPPING_DYNAMIC_MAPPINGS_BATCHING_ENABLED_SETTING.getKey();
        assertFalse(MetadataMappingService.PUT_MAPPING_DYNAMIC_MAPPINGS_BATCHING_ENABLED_SETTING.get(Settings.EMPTY));
        final Index index = createIndexWithExistingField("test");
        try {
            for (boolean enabled : List.of(false, true)) {
                assertAcked(
                    client().admin()
                        .cluster()
                        .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                        .setPersistentSettings(Settings.builder().put(settingKey, enabled))
                );
                final String field = "field_" + enabled;
                assertAcked(indicesAdmin().preparePutMapping("test").setSource(field, "type=long"));
                assertThat(fieldTypeName(getInstanceFromNode(ClusterService.class).state(), index, field), equalTo("long"));
            }
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().putNull(settingKey))
                .get();
        }
    }

    public void testGroupCapturesRecoveryWarnings() throws Exception {
        final IndexMetadata indexMetadata = IndexMetadata.builder("unallocated")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping("""
                {"_doc":{"dynamic_templates":[
                  {"duplicate":{"match":"foo","mapping":{"type":"keyword"}}},
                  {"duplicate":{"match":"bar","mapping":{"type":"text"}}}
                ]}}
                """)
            .build();
        final ClusterState initial = ClusterState.builder(getInstanceFromNode(ClusterService.class).state())
            .metadata(Metadata.builder().put(ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadata, false)))
            .build();
        assertNull(getInstanceFromNode(IndicesService.class).indexService(indexMetadata.getIndex()));
        final ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        final String expectedWarning =
            "Dynamic template [duplicate] in index [unallocated] is defined more than once. It is not defined which of the"
                + " duplicate definitions takes effect. Defining multiple dynamic templates with the same name"
                + " will be rejected in a future version.";
        final List<HeaderCapturingTaskContext> contexts = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            contexts.add(
                new HeaderCapturingTaskContext(
                    new PutMappingClusterStateUpdateTask(
                        request(true, field("field" + i, "long"), indexMetadata.getIndex()),
                        ActionListener.noop(),
                        new HashMap<>()
                    ),
                    threadContext
                )
            );
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.addResponseHeader("existing", "preserved");
            newExecutor(IndexSettingProviders.EMPTY, true).execute(
                new ClusterStateTaskExecutor.BatchExecutionContext<>(initial, contexts, threadContext::newStoredContext)
            );
            assertEquals(Map.of("existing", List.of("preserved")), threadContext.getResponseHeaders());
            assertTrue(contexts.stream().allMatch(context -> context.completed));
            assertEquals(
                List.of(expectedWarning),
                contexts.getFirst().warnings.stream()
                    .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, true))
                    .toList()
            );
            assertTrue(contexts.getLast().warnings.isEmpty());
        }
        // ESTestCase also registers its own warning context, independently of the node's task contexts.
        assertWarnings(expectedWarning);
    }

    /**
     * Uses real thread-context scopes because the executor test utility's contexts do not capture headers.
     */
    private static class HeaderCapturingTaskContext implements ClusterStateTaskExecutor.TaskContext<PutMappingClusterStateUpdateTask> {
        private final PutMappingClusterStateUpdateTask task;
        private final ThreadContext threadContext;
        private final List<String> warnings = new ArrayList<>();
        private boolean completed;

        HeaderCapturingTaskContext(PutMappingClusterStateUpdateTask task, ThreadContext threadContext) {
            this.task = task;
            this.threadContext = threadContext;
        }

        @Override
        public PutMappingClusterStateUpdateTask getTask() {
            return task;
        }

        @Override
        public void success(Runnable onPublicationSuccess, ClusterStateAckListener listener) {
            assertFalse(completed);
            assertSame(task, listener);
            completed = true;
        }

        @Override
        public void success(Runnable onPublicationSuccess) {
            throw new AssertionError("expected acknowledgement listener");
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer) {
            throw new AssertionError("unexpected publication consumer");
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener listener) {
            throw new AssertionError("unexpected publication consumer");
        }

        @Override
        public void onFailure(Exception failure) {
            throw new AssertionError("unexpected task failure", failure);
        }

        @Override
        public Releasable captureResponseHeaders() {
            final ThreadContext.StoredContext storedContext = threadContext.newStoredContext();
            return () -> {
                try (storedContext) {
                    warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
                }
            };
        }
    }

    /**
     * A real {@link IndexSettingProvider} that records its invocations so the test can assert how often it is called and with what mapper.
     */
    private static class RecordingIndexSettingProvider implements IndexSettingProvider {
        final List<DocumentMapper> mappers = new ArrayList<>();
        Predicate<DocumentMapper> failOnMapper = mapper -> false;

        @Override
        public void provideAdditionalSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            boolean registryInstalledTemplate,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings,
            IndexVersion indexVersion,
            Settings.Builder additionalSettings
        ) {}

        @Override
        public void onUpdateMappings(IndexMetadata indexMetadata, DocumentMapper documentMapper, Settings.Builder additionalSettings) {
            mappers.add(documentMapper);
            if (failOnMapper.test(documentMapper)) {
                throw new IllegalStateException("simulated index setting provider failure");
            }
        }

        @Override
        public Set<String> affectedSettings() {
            return Set.of();
        }
    }

    public void testRandomizedBatchesMatchPerTaskExecution() throws Exception {
        final List<Index> indices = List.of(
            createIndexWithExistingField("test1"),
            createIndexWithExistingField("test2"),
            createIndexWithExistingField("test3")
        );
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        for (int iteration = 0; iteration < 10; iteration++) {
            final List<PutMappingClusterStateUpdateRequest> requests = new ArrayList<>();
            for (int i = randomIntBetween(1, 40); i > 0; i--) {
                final String properties = switch (randomIntBetween(0, 5)) {
                    case 0 -> "\"existing\":{\"type\":\"object\",\"properties\":{\"sub\":{\"type\":\"long\"}}}";
                    case 1 -> field("field" + randomIntBetween(0, 5) + ".sub", randomFrom("long", "keyword"));
                    case 2, 3 -> field("field" + randomIntBetween(0, 5), randomFrom("long", "keyword", "boolean"));
                    case 4 -> field("existing", randomFrom("keyword", "long"));
                    case 5 -> field("other" + randomIntBetween(0, 20), "long");
                    default -> throw new AssertionError();
                };
                final boolean autoUpdate = randomIntBetween(0, 9) != 0;
                if (randomIntBetween(0, 9) == 0) {
                    final Index[] targets = randomSubsetOf(randomIntBetween(2, 3), indices).toArray(Index[]::new);
                    requests.add(request(autoUpdate, properties, targets));
                } else {
                    requests.add(request(autoUpdate, properties, randomFrom(indices)));
                }
            }

            final ExecutionOutcome batched = execute(initial, true, requests);
            final ExecutionOutcome sequential = execute(initial, false, requests);

            final String description = requests.stream()
                .map(r -> r.autoUpdate() + " " + Arrays.toString(r.indices()) + " " + r.source())
                .collect(Collectors.joining("\n"));
            assertThat(description, batched.taskResults(), equalTo(sequential.taskResults()));
            for (Index index : indices) {
                assertThat(description, mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
            }
        }
    }

    private static List<PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        }), new HashMap<>()));
    }

}
