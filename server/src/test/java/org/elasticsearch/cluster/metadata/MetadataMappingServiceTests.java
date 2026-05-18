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
import org.elasticsearch.cluster.metadata.MetadataMappingService.PutMappingClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
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

    private static List<PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        })));
    }

}
