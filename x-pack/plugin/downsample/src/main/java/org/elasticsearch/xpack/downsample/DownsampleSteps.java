/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RerouteBehavior;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackFeatures;

import java.util.Map;

/**
 * The cluster level operations of a downsample operation on the target index, each one idempotent. They are shared by
 * {@link TransportDownsampleAction}, which chains them within one request, and by the system index orchestration, which runs
 * them one phase at a time and can re-run a phase after a failure or a master failover.
 */
public class DownsampleSteps {

    private static final Logger logger = LogManager.getLogger(DownsampleSteps.class);

    /**
     * The result of reading and validating the source index mapping: the mapping of the target index and the fields each
     * shard task needs.
     */
    public record Prepared(String targetMapping, TimeSeriesFields fields) {}

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final IndicesService indicesService;
    private final FeatureService featureService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;
    private final MasterServiceTaskQueue<TransportDownsampleAction.DownsampleClusterStateUpdateTask> taskQueue;

    DownsampleSteps(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        IndicesService indicesService,
        FeatureService featureService,
        MetadataCreateIndexService metadataCreateIndexService,
        IndexScopedSettings indexScopedSettings,
        MasterServiceTaskQueue<TransportDownsampleAction.DownsampleClusterStateUpdateTask> taskQueue
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;
        this.featureService = featureService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
        this.taskQueue = taskQueue;
    }

    /**
     * Replicas of the target index while it is being built: none in stateful (it speeds up indexing), one in stateless (where
     * indices without replicas are not readable).
     */
    public int minNumReplicas() {
        return DiscoveryNode.isStateless(clusterService.getSettings()) ? 1 : 0;
    }

    /**
     * Reads the source mapping, validates the downsample configuration against it, and derives the target mapping and the
     * fields of the shard tasks. Fails with an {@link ActionRequestValidationException} if the source has no dimensions.
     */
    public void prepare(
        IndexMetadata sourceIndexMetadata,
        DownsampleConfig config,
        ClusterState state,
        TimeValue masterNodeTimeout,
        @Nullable TaskId parentTask,
        ActionListener<Prepared> listener
    ) {
        final String sourceIndexName = sourceIndexMetadata.getIndex().getName();
        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest(masterNodeTimeout).indices(sourceIndexName);
        if (parentTask != null) {
            getMappingsRequest.setParentTask(parentTask);
        }
        client.admin().indices().getMappings(getMappingsRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            final Map<String, Object> sourceIndexMappings = response.mappings()
                .entrySet()
                .stream()
                .filter(entry -> sourceIndexName.equals(entry.getKey()))
                .findFirst()
                .map(mappingMetadata -> mappingMetadata.getValue().sourceAsMap())
                .orElseThrow(() -> new IllegalArgumentException("No mapping found for downsample source index [" + sourceIndexName + "]"));

            final MapperService mapperService = indicesService.createIndexMapperServiceForValidation(sourceIndexMetadata);
            mapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                new CompressedXContent(sourceIndexMappings),
                MapperService.MergeReason.INDEX_TEMPLATE
            );
            TransportDownsampleAction.validateDownsamplingConfiguration(mapperService, config, sourceIndexMetadata);
            final TimeSeriesFields fields = new TimeSeriesFields.Collector(mapperService, config.getTimestampField()).collect(
                sourceIndexMappings
            );
            if (fields.dimensionFields().length == 0) {
                ActionRequestValidationException validationException = new ActionRequestValidationException();
                validationException.addValidationError("Index [" + sourceIndexName + "] does not contain any dimension fields");
                throw validationException;
            }
            final boolean isDefaultMetricDeprecated = featureService.clusterHasFeature(
                state,
                XPackFeatures.AGGREGATE_METRIC_DOUBLE_DEPRECATED_DEFAULT_METRIC
            );
            final String mapping = TransportDownsampleAction.createDownsampleIndexMapping(
                config,
                sourceIndexMappings,
                isDefaultMetricDeprecated
            );
            delegate.onResponse(new Prepared(mapping, fields));
        }));
    }

    /**
     * The params of the shard task that downsamples one shard of the source index.
     */
    public static DownsampleShardTaskParams shardTaskParams(
        DownsampleConfig config,
        IndexMetadata sourceIndexMetadata,
        String targetIndexName,
        TimeSeriesFields fields,
        ShardId shardId
    ) {
        return TransportDownsampleAction.createPersistentTaskParams(
            config,
            sourceIndexMetadata,
            targetIndexName,
            fields.metricFields(),
            fields.labelFields(),
            fields.dimensionFields(),
            fields.multiFieldSources(),
            shardId
        );
    }

    /**
     * Creates the target index: hidden, without refresh, with {@code index.downsample.status: started}, and with the settings
     * and metadata copied from the source. Fails with a {@link org.elasticsearch.ResourceAlreadyExistsException} if it exists.
     */
    public void createTargetIndex(
        ProjectId projectId,
        String downsampleIndexName,
        int minNumReplicas,
        IndexMetadata sourceIndexMetadata,
        String mapping,
        DownsampleConfig config,
        TimeValue masterNodeTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        var downsampleInterval = config.getInterval().toString();
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, sourceIndexMetadata.getNumberOfShards())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, minNumReplicas)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), DownsampleTaskStatus.STARTED)
            .put(IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL.getKey(), downsampleInterval)
            .put(IndexMetadata.INDEX_DOWNSAMPLE_METHOD.getKey(), config.getSamplingMethodOrDefault().toString())
            .put(IndexSettings.MODE.getKey(), sourceIndexMetadata.getIndexMode())
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), sourceIndexMetadata.getRoutingPaths())
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                sourceIndexMetadata.getSettings().get(IndexSettings.TIME_SERIES_START_TIME.getKey())
            )
            .put(
                IndexSettings.TIME_SERIES_END_TIME.getKey(),
                sourceIndexMetadata.getSettings().get(IndexSettings.TIME_SERIES_END_TIME.getKey())
            );
        if (sourceIndexMetadata.getTimeSeriesDimensions().isEmpty() == false) {
            builder.putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), sourceIndexMetadata.getTimeSeriesDimensions());
        }
        if (sourceIndexMetadata.getSettings().hasValue(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())) {
            builder.put(
                MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(),
                sourceIndexMetadata.getSettings().get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())
            );
        }
        if (sourceIndexMetadata.getSettings().hasValue(FieldMapper.IGNORE_MALFORMED_SETTING.getKey())) {
            builder.put(
                FieldMapper.IGNORE_MALFORMED_SETTING.getKey(),
                sourceIndexMetadata.getSettings().get(FieldMapper.IGNORE_MALFORMED_SETTING.getKey())
            );
        }

        CreateIndexClusterStateUpdateRequest createIndexClusterStateUpdateRequest = new CreateIndexClusterStateUpdateRequest(
            "downsample",
            projectId,
            downsampleIndexName,
            downsampleIndexName
        ).settings(builder.build()).settingsSystemProvided(true).mappings(mapping).waitForActiveShards(ActiveShardCount.ONE);
        var delegate = new AllocationActionListener<>(listener, threadPool.getThreadContext());
        taskQueue.submitTask(
            "create-downsample-index [" + downsampleIndexName + "]",
            new TransportDownsampleAction.DownsampleClusterStateUpdateTask(listener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    logger.debug("Creating downsample index [{}]", downsampleIndexName);
                    return metadataCreateIndexService.applyCreateIndexRequest(
                        currentState,
                        createIndexClusterStateUpdateRequest,
                        true,
                        // Copy index metadata from source index to downsample index
                        (builder, indexMetadata) -> builder.put(
                            TransportDownsampleAction.copyIndexMetadata(sourceIndexMetadata, indexMetadata, indexScopedSettings)
                        ),
                        RerouteBehavior.PERFORM_REROUTE,
                        delegate.reroute()
                    );
                }
            },
            masterNodeTimeout
        );
    }

    /**
     * Makes the target index read-only, restores the number of replicas and the {@code index.hidden} setting of the source.
     */
    public void updateTargetSettings(
        String downsampleIndexName,
        int minNumReplicas,
        IndexMetadata sourceIndexMetadata,
        @Nullable TaskId parentTask,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true);
        // Number of replicas had been previously set to 0 to speed up index population
        if (sourceIndexMetadata.getNumberOfReplicas() > 0 && minNumReplicas == 0) {
            settings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, sourceIndexMetadata.getNumberOfReplicas());
        }
        // Setting index.hidden has been initially set to true. We revert this to the value of the
        // source index
        if (sourceIndexMetadata.isHidden() == false) {
            if (sourceIndexMetadata.getSettings().keySet().contains(IndexMetadata.SETTING_INDEX_HIDDEN)) {
                settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, false);
            } else {
                settings.putNull(IndexMetadata.SETTING_INDEX_HIDDEN);
            }
        }
        UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(settings.build(), downsampleIndexName);
        if (parentTask != null) {
            updateSettingsReq.setParentTask(parentTask);
        }
        client.admin().indices().updateSettings(updateSettingsReq, listener);
    }

    public void refresh(String downsampleIndexName, @Nullable TaskId parentTask, ActionListener<BroadcastResponse> listener) {
        final RefreshRequest request = new RefreshRequest(downsampleIndexName);
        if (parentTask != null) {
            request.setParentTask(parentTask);
        }
        client.admin().indices().refresh(request, listener);
    }

    /**
     * Sets {@code index.downsample.status: success} on the target index, the commit point of a downsample operation.
     */
    public void markSuccess(
        ProjectId projectId,
        String downsampleIndexName,
        TimeValue timeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "update-downsample-metadata [" + downsampleIndexName + "]",
            new TransportDownsampleAction.DownsampleClusterStateUpdateTask(listener) {

                @Override
                public ClusterState execute(ClusterState currentState) {
                    logger.debug("Updating downsample index status for [{}]", downsampleIndexName);
                    final ProjectMetadata project = currentState.metadata().getProject(projectId);
                    final IndexMetadata downsampleIndex = project.index(downsampleIndexName);
                    if (downsampleIndex == null) {
                        throw new IllegalStateException(
                            "Failed to update downsample status because [" + downsampleIndexName + "] does not exist"
                        );
                    }
                    if (IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(downsampleIndex.getSettings()) == DownsampleTaskStatus.SUCCESS) {
                        return currentState;
                    }

                    final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(project);
                    projectBuilder.updateSettings(
                        Settings.builder()
                            .put(downsampleIndex.getSettings())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), DownsampleTaskStatus.SUCCESS)
                            .build(),
                        downsampleIndexName
                    );
                    return ClusterState.builder(currentState).putProjectMetadata(projectBuilder).build();
                }
            },
            timeout
        );
    }

    /**
     * Flushes the target index. Best effort: the target index is complete even if the flush fails, so this never fails.
     */
    public void flush(String downsampleIndexName, @Nullable TaskId parentTask, ActionListener<Void> listener) {
        logger.debug("Preparing to flush downsample index [{}]", downsampleIndexName);
        FlushRequest request = new FlushRequest(downsampleIndexName);
        if (parentTask != null) {
            request.setParentTask(parentTask);
        }
        client.admin().indices().flush(request, ActionListener.wrap(response -> listener.onResponse(null), e -> {
            logger.error("Failed to flush downsample index [" + downsampleIndexName + "]", e);
            listener.onResponse(null);
        }));
    }
}
