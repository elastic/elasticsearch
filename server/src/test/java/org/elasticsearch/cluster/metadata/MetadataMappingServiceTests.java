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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
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
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
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
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);

        final ClusterState resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            initialState,
            mappingService.new PutMappingExecutor(),
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

        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
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
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
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
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
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
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor(
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
            }))
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
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
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
        preflightCache.put(index, buildCacheEntry(indicesService, indexMetadata, mapping));
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
    }

    /** Batch: second task's unconsumed cache entry for a shared index is closed on success. */
    public void testBatchingSecondTaskCacheEntryIsClearedOnSuccess() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        final String mapping1 = """
            { "properties": { "field1": { "type": "keyword" }}}""";
        final String mapping2 = """
            { "properties": { "field2": { "type": "keyword" }}}""";

        // Task 2 carries a pre-flight cache entry for the same index as task 1. After task 1
        // succeeds, the index is already in indexMapperServices, so task 2's cache entry is
        // never consumed and must be closed by the success-path finally block.
        final Map<Index, PreflightCacheEntry> preflightCache2 = new HashMap<>();
        preflightCache2.put(index, buildCacheEntry(indicesService, indexMetadata, mapping2));

        final PutMappingClusterStateUpdateRequest request1 = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            mapping1,
            false,
            index
        );
        final PutMappingClusterStateUpdateRequest request2 = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            mapping2,
            false,
            index
        );

        ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            List.of(
                new PutMappingClusterStateUpdateTask(request1, ActionListener.wrap(r -> {}, e -> {}), new HashMap<>()),
                new PutMappingClusterStateUpdateTask(request2, ActionListener.wrap(r -> {}, e -> {}), preflightCache2)
            )
        );

        // Task 2's pre-flight cache entry was unconsumed; the success-path finally block must have
        // closed and cleared it.
        assertTrue("task2 preflightCache should be cleared on success", preflightCache2.isEmpty());
    }

    /** Shared body: builds a stale cache entry (versions offset by deltas) and asserts fall-back. */
    private void runCacheMissTest(long mappingVersionDelta, long settingsVersionDelta) throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
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
        final MapperService service = indicesService.createIndexMapperServiceForValidation(indexMetadata);
        service.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
        final CompressedXContent preUpdateSource = service.documentMapper() != null ? service.documentMapper().mappingSource() : null;
        service.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        return new PreflightCacheEntry(mappingVersion, settingsVersion, preUpdateSource, service);
    }

    private static List<PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        }), new HashMap<>()));
    }

}
