/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.action.RollupActionRequestValidationException;
import org.elasticsearch.xpack.core.rollup.action.RollupIndexerAction;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The master rollup action that coordinates
 *  -  creating the rollup index
 *  -  calling {@link TransportRollupIndexerAction} to index rollup documents
 *  -  cleaning up state
 */
public class TransportRollupAction extends AcknowledgedTransportMasterNodeAction<RollupAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportRollupAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final MetadataCreateIndexService metadataCreateIndexService;

    /**
     * This is the cluster state task executor for cluster state update actions.
     */
    private static final ClusterStateTaskExecutor<RollupClusterStateUpdateTask> STATE_UPDATE_TASK_EXECUTOR = (
        currentState,
        taskContexts) -> {
        ClusterState state = currentState;
        for (final var taskContext : taskContexts) {
            try {
                final var task = taskContext.getTask();
                state = task.execute(state);
                taskContext.success(() -> task.listener.onResponse(AcknowledgedResponse.TRUE));
            } catch (Exception e) {
                taskContext.onFailure(e);
            }
        }
        return state;
    };

    @Inject
    public TransportRollupAction(
        Client client,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        MetadataCreateIndexService metadataCreateIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RollupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RollupAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.clusterService = clusterService;
        this.metadataCreateIndexService = metadataCreateIndexService;
    }

    @Override
    protected void masterOperation(
        Task task,
        RollupAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String sourceIndexName = request.getSourceIndex();
        IndexMetadata sourceIndexMetadata = state.getMetadata().index(sourceIndexName);
        // Assert source index exists
        if (sourceIndexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(sourceIndexName));
            return;
        }

        // Assert source index is a time_series index
        if (IndexSettings.MODE.get(sourceIndexMetadata.getSettings()) != IndexMode.TIME_SERIES) {
            listener.onFailure(
                new ElasticsearchException(
                    "Rollup requires setting ["
                        + IndexSettings.MODE.getKey()
                        + "="
                        + IndexMode.TIME_SERIES
                        + "] for index ["
                        + sourceIndexName
                        + "]"
                )
            );
            return;
        }

        // Assert source index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndexName) == false) {
            listener.onFailure(
                new ElasticsearchException(
                    "Rollup requires setting [" + IndexMetadata.SETTING_BLOCKS_WRITE + " = true] for index [" + sourceIndexName + "]"
                )
            );
            return;
        }

        final String rollupIndexName = request.getRollupIndex();
        // Assert rollup index does not exist
        MetadataCreateIndexService.validateIndexName(rollupIndexName, state);

        // Rollup will perform the following tasks:
        // 1. Extract rollup config from source index field caps
        // 2. Create the rollup index
        // 3. Run rollup indexer
        // 4. Make rollup index read-only and set replicas
        // 5. Refresh rollup index
        // 6. Mark rollup index as "completed successfully"
        // 7. Force-merge the rollup index to a single segment
        // At any point if there is an issue, delete the rollup index

        // 1. Extract rollup config from source index field caps
        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices(sourceIndexName).fields("*");
        final TaskId parentTask = new TaskId(clusterService.localNode().getId(), task.getId());
        fieldCapsRequest.setParentTask(parentTask);
        client.fieldCaps(fieldCapsRequest, ActionListener.wrap(fieldCapsResponse -> {
            final Map<String, FieldCapabilities> dimensionFieldCaps = new HashMap<>();
            final Map<String, FieldCapabilities> metricFieldCaps = new HashMap<>();
            for (Map.Entry<String, Map<String, FieldCapabilities>> e : fieldCapsResponse.get().entrySet()) {
                String field = e.getKey();
                /*
                 * Rollup runs on a single index, and we do not expect multiple mappings for the same
                 * field. So, it is safe to select the first and only value of the FieldCapsResponse
                 * by running: e.getValue().values().iterator().next()
                 */
                if (e.getValue().size() != 1) {
                    throw new IllegalStateException(
                        "Cannot parse mapping for field [" + field + "] at source index [" + sourceIndexName + "]"
                    );
                }
                FieldCapabilities fieldCaps = e.getValue().values().iterator().next();
                if (fieldCaps.isDimension()) {
                    dimensionFieldCaps.put(field, fieldCaps);
                } else if (e.getValue().values().iterator().next().getMetricType() != null) {
                    metricFieldCaps.put(field, fieldCaps);
                } else {
                    // TODO: Field is not a dimension or a metric. Treat it as a tag
                }
            }

            RollupActionRequestValidationException validationException = new RollupActionRequestValidationException();
            if (dimensionFieldCaps.isEmpty()) {
                validationException.addValidationError("Index [" + sourceIndexName + "] does not contain any dimension fields");
            }
            if (metricFieldCaps.isEmpty()) {
                validationException.addValidationError("Index [" + sourceIndexName + "] does not contain any metric fields");
            }

            if (validationException.validationErrors().isEmpty() == false) {
                listener.onFailure(validationException);
                return;
            }

            final String mapping;
            try {
                mapping = createRollupIndexMapping(request.getRollupConfig(), dimensionFieldCaps, metricFieldCaps);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }

            // 2. Create rollup index
            createRollupIndex(rollupIndexName, sourceIndexMetadata, mapping, request, ActionListener.wrap(createIndexResp -> {
                if (createIndexResp.isAcknowledged()) {
                    // 3. Rollup index created. Run rollup indexer
                    RollupIndexerAction.Request rollupIndexerRequest = new RollupIndexerAction.Request(
                        request,
                        dimensionFieldCaps.keySet().toArray(new String[0]),
                        metricFieldCaps.keySet().toArray(new String[0])
                    );
                    rollupIndexerRequest.setParentTask(parentTask);
                    client.execute(RollupIndexerAction.INSTANCE, rollupIndexerRequest, ActionListener.wrap(indexerResp -> {
                        if (indexerResp.isCreated()) {
                            // 4. Make rollup index read-only and set the correct number of replicas
                            final Settings settings = Settings.builder()
                                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, sourceIndexMetadata.getNumberOfReplicas())
                                .build();
                            UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(settings, rollupIndexName);
                            updateSettingsReq.setParentTask(parentTask);
                            client.admin().indices().updateSettings(updateSettingsReq, ActionListener.wrap(updateSettingsResponse -> {
                                if (updateSettingsResponse.isAcknowledged()) {
                                    // 5. Refresh rollup index
                                    refreshIndex(rollupIndexName, parentTask, ActionListener.wrap(refreshIndexResponse -> {
                                        if (refreshIndexResponse.getFailedShards() == 0) {
                                            // 6. Mark rollup index as "completed successfully"
                                            updateRollupMetadata(sourceIndexName, rollupIndexName, request, ActionListener.wrap(resp -> {
                                                if (resp.isAcknowledged()) {
                                                    // 7. Force-merge the rollup index to a single segment
                                                    forceMergeIndex(
                                                        rollupIndexName,
                                                        parentTask,
                                                        ActionListener.wrap(
                                                            mergeIndexResp -> listener.onResponse(AcknowledgedResponse.TRUE),
                                                            e -> {
                                                                /*
                                                                 * At this point rollup has been created successfully even if force-merge
                                                                 * fails. So, we should not fail the rollup operation.
                                                                 */
                                                                logger.error(
                                                                    "Failed to force-merge rollup index [" + rollupIndexName + "]",
                                                                    e
                                                                );
                                                                listener.onResponse(AcknowledgedResponse.TRUE);
                                                            }
                                                        )
                                                    );
                                                } else {
                                                    deleteRollupIndex(
                                                        sourceIndexName,
                                                        rollupIndexName,
                                                        parentTask,
                                                        listener,
                                                        new ElasticsearchException(
                                                            "Failed to publish new cluster state with rollup metadata"
                                                        )
                                                    );
                                                }
                                            },
                                                e -> deleteRollupIndex(
                                                    sourceIndexName,
                                                    rollupIndexName,
                                                    parentTask,
                                                    listener,
                                                    new ElasticsearchException(
                                                        "Failed to publish new cluster state with rollup metadata",
                                                        e
                                                    )
                                                )
                                            ));
                                        } else {
                                            deleteRollupIndex(
                                                sourceIndexName,
                                                rollupIndexName,
                                                parentTask,
                                                listener,
                                                new ElasticsearchException("Failed to refresh rollup index [" + rollupIndexName + "]")
                                            );
                                        }
                                    },
                                        e -> deleteRollupIndex(
                                            sourceIndexName,
                                            rollupIndexName,
                                            parentTask,
                                            listener,
                                            new ElasticsearchException("Failed to refresh rollup index [" + rollupIndexName + "]", e)
                                        )
                                    ));
                                } else {
                                    deleteRollupIndex(
                                        sourceIndexName,
                                        rollupIndexName,
                                        parentTask,
                                        listener,
                                        new ElasticsearchException("Unable to update settings of rollup index [" + rollupIndexName + "]")
                                    );
                                }
                            },
                                e -> deleteRollupIndex(
                                    sourceIndexName,
                                    rollupIndexName,
                                    parentTask,
                                    listener,
                                    new ElasticsearchException("Unable to update settings of rollup index [" + rollupIndexName + "]", e)
                                )
                            ));
                        } else {
                            deleteRollupIndex(
                                sourceIndexName,
                                rollupIndexName,
                                parentTask,
                                listener,
                                new ElasticsearchException("Unable to index into rollup index [" + rollupIndexName + "]")
                            );
                        }
                    }, e -> deleteRollupIndex(sourceIndexName, rollupIndexName, parentTask, listener, e)));
                } else {
                    listener.onFailure(new ElasticsearchException("Failed to create rollup index [" + rollupIndexName + "]"));
                }
            }, listener::onFailure));
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(RollupAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * This method creates the mapping for the rollup index, based on the
     * mapping (dimensions and metrics) from the source index, as well as the
     * rollup configuration.
     *
     * @param config the rollup configuration
     * @param dimensionFieldCaps a map with the field name as key and the fields caps response as value
     *                  for the dimension fields of the source index
     * @param metricFieldCaps a map with the field name as key and the fields caps response as value
     *                for the metric fields of the source index
     *
     * @return the mapping of the rollup index
     */
    public static String createRollupIndexMapping(
        final RollupActionConfig config,
        final Map<String, FieldCapabilities> dimensionFieldCaps,
        final Map<String, FieldCapabilities> metricFieldCaps
    ) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder = getDynamicTemplates(builder);

        builder.startObject("properties");

        String timestampField = config.getTimestampField();
        String dateIntervalType = config.getIntervalType();
        String dateInterval = config.getInterval().toString();
        String tz = config.getTimeZone();

        builder.startObject(timestampField)
            .field("type", DateFieldMapper.CONTENT_TYPE)
            .startObject("meta")
            .field(dateIntervalType, dateInterval)
            .field(RollupActionConfig.TIME_ZONE, tz)
            .endObject()
            .endObject();

        for (Map.Entry<String, FieldCapabilities> e : dimensionFieldCaps.entrySet()) {
            builder.startObject(e.getKey())
                .field("type", e.getValue().getType())
                .field(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM, true)
                .endObject();
        }

        for (Map.Entry<String, FieldCapabilities> e : metricFieldCaps.entrySet()) {
            TimeSeriesParams.MetricType metricType = e.getValue().getMetricType();
            if (metricType == TimeSeriesParams.MetricType.counter) {
                // For counters we keep the same field type, because they store
                // only one value (the last value of the counter)
                builder.startObject(e.getKey())
                    .field("type", e.getValue().getType())
                    .field(TimeSeriesParams.TIME_SERIES_METRIC_PARAM, metricType)
                    .endObject();
            } else {
                List<String> aggs = List.of(metricType.supportedAggs());
                // We choose max as the default metric
                String defaultMetric = aggs.contains("max") ? "max" : aggs.get(0);
                builder.startObject(e.getKey())
                    .field("type", AggregateDoubleMetricFieldMapper.CONTENT_TYPE)
                    .stringListField(AggregateDoubleMetricFieldMapper.Names.METRICS, aggs)
                    .field(AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC, defaultMetric)
                    .field(TimeSeriesParams.TIME_SERIES_METRIC_PARAM, metricType)
                    .endObject();
            }
        }

        builder.endObject();
        builder.endObject();
        return XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
    }

    /**
     * Copy index metadata from the source index to the rollup index.
     */
    private IndexMetadata.Builder copyIndexMetadata(IndexMetadata sourceIndexMetadata, IndexMetadata rollupIndexMetadata) {
        String sourceIndexName = sourceIndexMetadata.getIndex().getName();

        /*
         * Add the source index name and UUID to the rollup index metadata.
         * If the source index is a rollup index, we will add the name and UUID
         * of the first index that we initially rolled up.
         */
        String originalIndexName = IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.exists(sourceIndexMetadata.getSettings())
            ? IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.get(sourceIndexMetadata.getSettings())
            : sourceIndexName;
        String originalIndexUuid = IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.exists(sourceIndexMetadata.getSettings())
            ? IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.get(sourceIndexMetadata.getSettings())
            : sourceIndexMetadata.getIndexUUID();

        // Copy time series index settings from original index
        List<String> indexRoutingPath = sourceIndexMetadata.getRoutingPaths();
        Instant startTime = IndexSettings.TIME_SERIES_START_TIME.get(sourceIndexMetadata.getSettings());
        Instant endTime = IndexSettings.TIME_SERIES_END_TIME.get(sourceIndexMetadata.getSettings());
        IndexMode indexMode = IndexSettings.MODE.get(sourceIndexMetadata.getSettings());

        return IndexMetadata.builder(rollupIndexMetadata)
            .settings(
                Settings.builder()
                    .put(rollupIndexMetadata.getSettings())
                    .put(IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.getKey(), originalIndexName)
                    .put(IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.getKey(), originalIndexUuid)
                    .put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), sourceIndexMetadata.isHidden())
                    // Add the time series index settings
                    .put(IndexSettings.MODE.getKey(), indexMode)
                    .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), indexRoutingPath)
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime.toString())
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime.toString())
            );
    }

    /**
     * Configure the dynamic templates to always map strings to the keyword field type.
     */
    private static XContentBuilder getDynamicTemplates(XContentBuilder builder) throws IOException {
        return builder.startArray("dynamic_templates")
            .startObject()
            .startObject("strings")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray();
    }

    private void createRollupIndex(
        String rollupIndexName,
        IndexMetadata sourceIndexMetadata,
        String mapping,
        RollupAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        CreateIndexClusterStateUpdateRequest createIndexClusterStateUpdateRequest = new CreateIndexClusterStateUpdateRequest(
            "rollup",
            rollupIndexName,
            rollupIndexName
        ).settings(
            /*
             * When creating the rollup index, we copy the index.number_of_shards from source index,
             * and we set the index.number_of_replicas to 0, to avoid replicating the index being built.
             * Also, we set the index.refresh_interval to -1.
             * We will set the correct number of replicas and refresh the index later.
             *
             * We should note that there is a risk of losing a node during the rollup process. In this
             * case rollup will fail.
             */
            Settings.builder()
                .put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), true)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, sourceIndexMetadata.getNumberOfShards())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                .put(IndexMetadata.INDEX_ROLLUP_STATUS.getKey(), IndexMetadata.RollupTaskStatus.STARTED)
                .build()
        ).mappings(mapping);
        clusterService.submitStateUpdateTask("create-rollup-index [" + rollupIndexName + "]", new RollupClusterStateUpdateTask(listener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return metadataCreateIndexService.applyCreateIndexRequest(
                    currentState,
                    createIndexClusterStateUpdateRequest,
                    true,
                    // Copy index metadata from source index to rollup index
                    (builder, rollupIndexMetadata) -> builder.put(copyIndexMetadata(sourceIndexMetadata, rollupIndexMetadata))
                );
            }
        }, ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()), STATE_UPDATE_TASK_EXECUTOR);
    }

    private void updateRollupMetadata(
        String sourceIndexName,
        String rollupIndexName,
        RollupAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // 6. Mark rollup index as "completed successfully" ("index.rollup.status": "success")
        clusterService.submitStateUpdateTask(
            "update-rollup-metadata [" + rollupIndexName + "]",
            new RollupClusterStateUpdateTask(listener) {

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder metadataBuilder = Metadata.builder(metadata);
                    Index rollupIndex = metadata.index(rollupIndexName).getIndex();
                    IndexMetadata rollupIndexMetadata = metadata.index(rollupIndex);

                    metadataBuilder.updateSettings(
                        Settings.builder()
                            .put(rollupIndexMetadata.getSettings())
                            .put(IndexMetadata.INDEX_ROLLUP_STATUS.getKey(), IndexMetadata.RollupTaskStatus.SUCCESS)
                            .build(),
                        rollupIndexName
                    );
                    return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
                }
            },
            ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
            STATE_UPDATE_TASK_EXECUTOR
        );
    }

    private void refreshIndex(String index, TaskId parentTask, ActionListener<RefreshResponse> listener) {
        RefreshRequest request = new RefreshRequest(index);
        request.setParentTask(parentTask);
        client.admin().indices().refresh(request, listener);
    }

    private void forceMergeIndex(String index, TaskId parentTask, ActionListener<ForceMergeResponse> listener) {
        ForceMergeRequest request = new ForceMergeRequest(index);
        request.maxNumSegments(1);
        request.setParentTask(parentTask);
        client.admin().indices().forceMerge(request, listener);
    }

    private void deleteRollupIndex(
        String sourceIndex,
        String rollupIndex,
        TaskId parentTask,
        ActionListener<AcknowledgedResponse> listener,
        Exception e
    ) {
        DeleteIndexRequest request = new DeleteIndexRequest(rollupIndex);
        request.setParentTask(parentTask);
        client.admin().indices().delete(request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (e == null && acknowledgedResponse.isAcknowledged()) {
                    listener.onResponse(acknowledgedResponse);
                } else {
                    listener.onFailure(new ElasticsearchException("Unable to rollup index [" + sourceIndex + "]", e));
                }
            }

            @Override
            public void onFailure(Exception deleteException) {
                listener.onFailure(new ElasticsearchException("Unable to delete the temporary rollup index [" + rollupIndex + "]", e));
            }
        });
    }

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private abstract static class RollupClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;

        RollupClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener) {
            this.listener = listener;
        }

        public abstract ClusterState execute(ClusterState currentState) throws Exception;

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
        }
    }
}
