/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.elasticsearch.xpack.downsample.DownsampleActionSingleNodeTests.randomSamplingMethod;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDownsampleActionTests extends ESTestCase {

    @Mock
    private ClusterService clusterService;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Client client;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private DownsampleMetrics downsampleMetrics;
    @Mock
    private ProjectResolver projectResolver;
    @Mock
    private PersistentTasksService persistentTaskService;
    @Mock
    private IndicesService indicesService;
    @Mock
    private MasterServiceTaskQueue<TransportDownsampleAction.DownsampleClusterStateUpdateTask> taskQueue;
    @Mock
    private MapperService mapperService;

    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "attributes.host": {
                "type": "keyword",
                "time_series_dimension": true
              },
              "metrics.cpu_usage": {
                "type": "double",
                "time_series_metric": "counter"
              }
            }
          }
        }""";

    private TransportDownsampleAction action;

    private AutoCloseable mocks;
    private String sourceIndex;
    private String targetIndex;
    private int primaryShards;
    private int replicaShards;
    private ProjectId projectId;
    private Task task;
    private IndicesAdminClient indicesAdminClient;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mocks = MockitoAnnotations.openMocks(this);
        action = new TransportDownsampleAction(
            client,
            indicesService,
            clusterService,
            mock(TransportService.class),
            threadPool,
            mock(MetadataCreateIndexService.class),
            new ActionFilters(Set.of()),
            projectResolver,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            persistentTaskService,
            downsampleMetrics,
            taskQueue,
            System::currentTimeMillis
        );
        indicesAdminClient = client.admin().indices();
        sourceIndex = "source-index";
        targetIndex = "downsample-index";
        primaryShards = randomIntBetween(1, 5);
        replicaShards = randomIntBetween(0, 3);
        projectId = randomProjectIdOrDefault();
        task = new Task(1, "type", "action", "description", null, null);

        // Initialise mocks for thread pool and cluster service
        var threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(clusterService.localNode()).thenReturn(DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), "node_name"));
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        // Mock refresh & flush requests
        Answer<Void> mockBroadcastResponse = invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<BroadcastResponse>) invocation.getArgument(1, ActionListener.class);
            listener.onResponse(new BroadcastResponse(primaryShards, primaryShards, 0, List.of()));
            return null;
        };
        doAnswer(mockBroadcastResponse).when(indicesAdminClient).refresh(any(), any());
        doAnswer(mockBroadcastResponse).when(indicesAdminClient).flush(any(), any());

        doAnswer(invocation -> {
            var updateTask = invocation.getArgument(1, TransportDownsampleAction.DownsampleClusterStateUpdateTask.class);
            updateTask.listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(taskQueue).submitTask(startsWith("create-downsample-index"), any(), any());

        // Mocks for mapping retrieval & merging
        when(indicesService.createIndexMapperServiceForValidation(any())).thenReturn(mapperService);
        MappedFieldType timestampFieldMock = mock(MappedFieldType.class);
        when(timestampFieldMock.meta()).thenReturn(Map.of());
        when(mapperService.fieldType(any())).thenReturn(timestampFieldMock);
        when(mapperService.mappingLookup()).thenReturn(MappingLookup.EMPTY);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetMappingsResponse>) invocation.getArgument(1, ActionListener.class);
            listener.onResponse(
                new GetMappingsResponse(
                    Map.of(sourceIndex, new MappingMetadata("_doc", XContentHelper.convertToMap(JsonXContent.jsonXContent, MAPPING, true)))
                )
            );
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.mappingSource()).thenReturn(CompressedXContent.fromJSON(MAPPING));
        when(mapperService.merge(anyString(), any(CompressedXContent.class), any())).thenReturn(documentMapper);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        mocks.close();
    }

    public void testDownsampling() {
        var projectMetadata = ProjectMetadata.builder(projectId)
            .put(createSourceIndexMetadata(sourceIndex, primaryShards, replicaShards))
            .build();

        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(projectMetadata)
            .blocks(ClusterBlocks.builder().addIndexBlock(projectId, sourceIndex, IndexMetadata.INDEX_WRITE_BLOCK))
            .build();

        when(projectResolver.getProjectMetadata(any(ClusterState.class))).thenReturn(projectMetadata);

        Answer<Void> mockPersistentTask = invocation -> {
            ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = invocation.getArgument(4);
            PersistentTasksCustomMetadata.PersistentTask<?> task1 = mock(PersistentTasksCustomMetadata.PersistentTask.class);
            when(task1.getId()).thenReturn(randomAlphaOfLength(10));
            DownsampleShardPersistentTaskState runningTaskState = new DownsampleShardPersistentTaskState(
                DownsampleShardIndexerStatus.COMPLETED,
                null
            );
            when(task1.getState()).thenReturn(runningTaskState);
            listener.onResponse(task1);
            return null;
        };
        doAnswer(mockPersistentTask).when(persistentTaskService).sendStartRequest(anyString(), anyString(), any(), any(), any());
        doAnswer(mockPersistentTask).when(persistentTaskService).waitForPersistentTaskCondition(any(), anyString(), any(), any(), any());
        doAnswer(invocation -> {
            var listener = invocation.getArgument(1, TransportDownsampleAction.UpdateDownsampleIndexSettingsActionListener.class);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesAdminClient).updateSettings(any(), any());
        assertSuccessfulUpdateDownsampleStatus(clusterState);

        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        action.masterOperation(
            task,
            new DownsampleAction.Request(
                ESTestCase.TEST_REQUEST_TIMEOUT,
                sourceIndex,
                targetIndex,
                TimeValue.ONE_HOUR,
                new DownsampleConfig(new DateHistogramInterval("5m"), randomSamplingMethod())
            ),
            clusterState,
            listener
        );
        safeGet(listener);
        verifyIndexFinalisation();
    }

    public void testDownsamplingWithShortCircuitAfterCreation() {
        var projectMetadata = ProjectMetadata.builder(projectId)
            .put(createSourceIndexMetadata(sourceIndex, primaryShards, replicaShards))
            .put(createTargetIndexMetadata(targetIndex, primaryShards, replicaShards))
            .build();

        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(projectMetadata)
            .blocks(ClusterBlocks.builder().addIndexBlock(projectId, sourceIndex, IndexMetadata.INDEX_WRITE_BLOCK))
            .build();

        when(projectResolver.getProjectMetadata(any(ClusterState.class))).thenReturn(projectMetadata);
        assertSuccessfulUpdateDownsampleStatus(clusterState);

        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        action.masterOperation(
            task,
            new DownsampleAction.Request(
                ESTestCase.TEST_REQUEST_TIMEOUT,
                sourceIndex,
                targetIndex,
                TimeValue.ONE_HOUR,
                new DownsampleConfig(new DateHistogramInterval("5m"), randomSamplingMethod())
            ),
            clusterState,
            listener
        );
        safeGet(listener);
        verifyIndexFinalisation();
    }

    public void testDownsamplingWithShortCircuitDuringCreation() {
        var projectMetadata = ProjectMetadata.builder(projectId)
            .put(createSourceIndexMetadata(sourceIndex, primaryShards, replicaShards))
            .build();

        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(projectMetadata)
            .blocks(ClusterBlocks.builder().addIndexBlock(projectId, sourceIndex, IndexMetadata.INDEX_WRITE_BLOCK))
            .build();

        when(projectResolver.getProjectMetadata(any(ClusterState.class))).thenReturn(projectMetadata);

        doAnswer(invocation -> {
            var updateTask = invocation.getArgument(1, TransportDownsampleAction.DownsampleClusterStateUpdateTask.class);
            updateTask.listener.onFailure(new ResourceAlreadyExistsException(targetIndex));
            return null;
        }).when(taskQueue).submitTask(startsWith("create-downsample-index"), any(), any());
        when(clusterService.state()).thenReturn(
            ClusterState.builder(clusterState)
                .putProjectMetadata(
                    ProjectMetadata.builder(projectMetadata).put(createTargetIndexMetadata(targetIndex, primaryShards, replicaShards))
                )
                .build()
        );
        assertSuccessfulUpdateDownsampleStatus(clusterService.state());

        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        action.masterOperation(
            task,
            new DownsampleAction.Request(
                ESTestCase.TEST_REQUEST_TIMEOUT,
                sourceIndex,
                targetIndex,
                TimeValue.ONE_HOUR,
                new DownsampleConfig(new DateHistogramInterval("5m"), randomSamplingMethod())
            ),
            clusterState,
            listener
        );
        safeGet(listener);
        verifyIndexFinalisation();
    }

    public void testDownsamplingWhenTargetIndexGetsDeleted() {
        var projectMetadata = ProjectMetadata.builder(projectId)
            .put(createSourceIndexMetadata(sourceIndex, primaryShards, replicaShards))
            .build();

        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(projectMetadata)
            .blocks(ClusterBlocks.builder().addIndexBlock(projectId, sourceIndex, IndexMetadata.INDEX_WRITE_BLOCK))
            .build();

        when(projectResolver.getProjectMetadata(any(ClusterState.class))).thenReturn(projectMetadata);

        Answer<Void> mockPersistentTask = invocation -> {
            ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = invocation.getArgument(4);
            PersistentTasksCustomMetadata.PersistentTask<?> task1 = mock(PersistentTasksCustomMetadata.PersistentTask.class);
            when(task1.getId()).thenReturn(randomAlphaOfLength(10));
            DownsampleShardPersistentTaskState runningTaskState = new DownsampleShardPersistentTaskState(
                DownsampleShardIndexerStatus.COMPLETED,
                null
            );
            when(task1.getState()).thenReturn(runningTaskState);
            listener.onResponse(task1);
            return null;
        };
        doAnswer(mockPersistentTask).when(persistentTaskService).sendStartRequest(anyString(), anyString(), any(), any(), any());
        doAnswer(mockPersistentTask).when(persistentTaskService).waitForPersistentTaskCondition(any(), anyString(), any(), any(), any());
        doAnswer(invocation -> {
            var listener = invocation.getArgument(1, TransportDownsampleAction.UpdateDownsampleIndexSettingsActionListener.class);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesAdminClient).updateSettings(any(), any());

        doAnswer(invocation -> {
            var updateTask = invocation.getArgument(1, TransportDownsampleAction.DownsampleClusterStateUpdateTask.class);
            ClusterStateTaskExecutorUtils.executeHandlingResults(
                clusterState,
                TransportDownsampleAction.STATE_UPDATE_TASK_EXECUTOR,
                List.of(updateTask),
                task1 -> {},
                TransportDownsampleAction.DownsampleClusterStateUpdateTask::onFailure
            );
            return null;
        }).when(taskQueue).submitTask(startsWith("update-downsample-metadata"), any(), any());
        IllegalStateException error = safeAwaitFailure(
            IllegalStateException.class,
            AcknowledgedResponse.class,
            listener -> action.masterOperation(
                task,
                new DownsampleAction.Request(
                    ESTestCase.TEST_REQUEST_TIMEOUT,
                    sourceIndex,
                    targetIndex,
                    TimeValue.ONE_HOUR,
                    new DownsampleConfig(new DateHistogramInterval("5m"), randomSamplingMethod())
                ),
                clusterState,
                listener
            )
        );
        assertThat(
            error.getMessage(),
            Matchers.startsWith("Failed to update downsample status because [" + targetIndex + "] does not exist")
        );
        verify(downsampleMetrics, never()).recordOperation(anyLong(), eq(DownsampleMetrics.ActionStatus.SUCCESS));
        verify(downsampleMetrics).recordOperation(anyLong(), eq(DownsampleMetrics.ActionStatus.FAILED));
        verify(indicesAdminClient).refresh(any(), any());
        verify(indicesAdminClient, never()).flush(any(), any());
        verify(indicesAdminClient, never()).forceMerge(any(), any());
    }

    private void verifyIndexFinalisation() {
        verify(downsampleMetrics).recordOperation(anyLong(), eq(DownsampleMetrics.ActionStatus.SUCCESS));
        verify(indicesAdminClient).refresh(any(), any());
        verify(indicesAdminClient).flush(any(), any());
        verify(indicesAdminClient, never()).forceMerge(any(), any());
    }

    private IndexMetadata.Builder createSourceIndexMetadata(String sourceIndex, int primaryShards, int replicaShards) {
        return IndexMetadata.builder(sourceIndex)
            .settings(
                indexSettings(IndexVersion.current(), randomUUID(), primaryShards, replicaShards).put(
                    IndexSettings.MODE.getKey(),
                    IndexMode.TIME_SERIES.getName()
                )
                    .put("index.routing_path", "dimensions")
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                    .put("index.time_series.start_time", "2021-01-01T00:00:00Z")
                    .put("index.time_series.end_time", "2022-01-01T00:00:00Z")
            );
    }

    private IndexMetadata.Builder createTargetIndexMetadata(String targetIndex, int primaryShards, int replicaShards) {
        return IndexMetadata.builder(targetIndex)
            .settings(
                indexSettings(IndexVersion.current(), randomUUID(), primaryShards, replicaShards).put(
                    IndexSettings.MODE.getKey(),
                    IndexMode.TIME_SERIES.getName()
                )
                    .put("index.routing_path", "dimensions")
                    .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.STARTED)
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            );

    }

    public void testCopyIndexMetadata() {
        // GIVEN
        final List<String> tiers = List.of(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD, DataTier.DATA_CONTENT);
        final IndexMetadata source = new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, 3))
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY, UUID.randomUUID().toString())
                .put(IndexSettings.DEFAULT_PIPELINE.getKey(), randomAlphaOfLength(15))
                .put(IndexSettings.FINAL_PIPELINE.getKey(), randomAlphaOfLength(11))
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), randomBoolean())
                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom(tiers))
                .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, randomAlphaOfLength(20))
                .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
        ).build();
        final IndexMetadata target = new IndexMetadata.Builder(randomAlphaOfLength(10)).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 4)
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY, UUID.randomUUID().toString())
                .put(IndexSettings.DEFAULT_PIPELINE.getKey(), randomAlphaOfLength(15))
                .put(IndexSettings.FINAL_PIPELINE.getKey(), randomAlphaOfLength(11))
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), randomBoolean())
                .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom(tiers))
                .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
        ).build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );

        // WHEN
        final Settings indexMetadata = TransportDownsampleAction.copyIndexMetadata(source, target, indexScopedSettings)
            .build()
            .getSettings();

        // THEN
        assertTargetSettings(target, indexMetadata);
        assertSourceSettings(source, indexMetadata);
        assertEquals(IndexMetadata.INDEX_UUID_NA_VALUE, indexMetadata.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID_KEY));
        assertFalse(LifecycleSettings.LIFECYCLE_NAME_SETTING.exists(indexMetadata));
    }

    private static void assertSourceSettings(final IndexMetadata indexMetadata, final Settings settings) {
        assertEquals(indexMetadata.getIndex().getName(), settings.get(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY));
        assertEquals(
            indexMetadata.getSettings().get(DataTier.TIER_PREFERENCE_SETTING.getKey()),
            settings.get(DataTier.TIER_PREFERENCE_SETTING.getKey())
        );
        assertEquals(indexMetadata.getSettings().get(IndexMetadata.LIFECYCLE_NAME), settings.get(IndexMetadata.LIFECYCLE_NAME));
        // NOTE: setting only in source (not forbidden, not to override): expect source setting is used
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
            settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );
    }

    private static void assertTargetSettings(final IndexMetadata indexMetadata, final Settings settings) {
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_SHARDS),
            settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexSettings.FINAL_PIPELINE.getKey()),
            settings.get(IndexSettings.FINAL_PIPELINE.getKey())
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexSettings.DEFAULT_PIPELINE.getKey()),
            settings.get(IndexSettings.DEFAULT_PIPELINE.getKey())
        );
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()),
            settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
        assertEquals(indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME), settings.get(LifecycleSettings.LIFECYCLE_NAME));
        // NOTE: setting both in source and target (not forbidden, not to override): expect target setting is used
        assertEquals(
            indexMetadata.getSettings().get(IndexMetadata.SETTING_CREATION_DATE),
            settings.get(IndexMetadata.SETTING_CREATION_DATE)
        );
    }

    public void testGetSupportedMetrics() {
        TimeSeriesParams.MetricType metricType = TimeSeriesParams.MetricType.GAUGE;
        Map<String, Object> fieldProperties = Map.of(
            "type",
            "aggregate_metric_double",
            "metrics",
            List.of("max", "sum"),
            "default_metric",
            "sum"
        );

        var supported = TransportDownsampleAction.getSupportedMetrics(metricType, fieldProperties);
        assertThat(supported.defaultMetric(), is("sum"));
        assertThat(supported.supportedMetrics(), is(List.of("max", "sum")));

        fieldProperties = Map.of("type", "integer");
        supported = TransportDownsampleAction.getSupportedMetrics(metricType, fieldProperties);
        assertThat(supported.defaultMetric(), is("max"));
        assertThat(supported.supportedMetrics(), is(List.of(metricType.supportedAggs())));
    }

    private void assertSuccessfulUpdateDownsampleStatus(ClusterState clusterState) {
        var projectMetadata = ProjectMetadata.builder(clusterState.metadata().getProject(projectId))
            .put(createSourceIndexMetadata(targetIndex, primaryShards, replicaShards))
            .build();

        var updatedClusterState = ClusterState.builder(clusterState).putProjectMetadata(projectMetadata).build();
        doAnswer(invocation -> {
            var updateTask = invocation.getArgument(1, TransportDownsampleAction.DownsampleClusterStateUpdateTask.class);
            ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                updatedClusterState,
                TransportDownsampleAction.STATE_UPDATE_TASK_EXECUTOR,
                List.of(updateTask)
            );
            return null;
        }).when(taskQueue).submitTask(startsWith("update-downsample-metadata"), any(), any());
    }
}
