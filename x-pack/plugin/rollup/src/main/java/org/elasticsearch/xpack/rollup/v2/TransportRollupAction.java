/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.tasks.Task;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The master rollup action that coordinates
 *  -  creating rollup temporary index
 *  -  calling {@link TransportRollupIndexerAction} to index rollup-ed documents
 *  -  cleaning up state
 */
public class TransportRollupAction extends AcknowledgedTransportMasterNodeAction<RollupAction.Request> {

    private static final Settings WRITE_BLOCKED_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();
    public static final String TMP_ROLLUP_INDEX_PREFIX = ".rollup-tmp-";

    private final Client client;
    private final ClusterService clusterService;
    private final MetadataCreateIndexService metadataCreateIndexService;

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
        }

        final String rollupIndexName = request.getRollupIndex();
        if (state.getMetadata().index(rollupIndexName) != null) {
            listener.onFailure(new ResourceAlreadyExistsException("Rollup index [{}] already exists.", rollupIndexName));
            return;
        }
        final String tmpIndexName = createTmpIndexName(rollupIndexName);
        if (state.getMetadata().index(tmpIndexName) != null) {
            listener.onFailure(new ResourceAlreadyExistsException("Temporary rollup index [{}] already exists.", tmpIndexName));
            return;
        }

        // 1. Extract rollup config from source index field caps
        // 2. Create a hidden temporary rollup index
        // 3. Run rollup indexer
        // 4. Make temp index read-only
        // 5. Clone the final rollup index from the temporary rollup index
        // 6. Publish rollup metadata and add rollup index to data stream
        // 7. Delete the source index
        // 8. Delete temporary rollup index
        // At any point if there is an issue, cleanup temp index

        // 1. Extract rollup config from source index field caps
        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices(sourceIndexName).fields("*");
        fieldCapsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.fieldCaps(fieldCapsRequest, ActionListener.wrap(fieldCapsResponse -> {
            final Map<String, FieldCapabilities> dimensionFieldCaps = new HashMap<>();
            final Map<String, FieldCapabilities> metricFieldCaps = new HashMap<>();
            /*
             * Rollup runs on a single index, and we do not expect multiple mappings for the same
             * field. So, it is safe to select the first and only value of the FieldCapsResponse
             * by running: e.getValue().values().iterator().next()
             */
            for (Map.Entry<String, Map<String, FieldCapabilities>> e : fieldCapsResponse.get().entrySet()) {
                String field = e.getKey();
                FieldCapabilities fieldCaps = e.getValue().values().iterator().next();
                if (fieldCaps.isDimension()) {
                    dimensionFieldCaps.put(field, fieldCaps);
                } else if (e.getValue().values().iterator().next().getMetricType() != null) {
                    metricFieldCaps.put(field, fieldCaps);
                }
            }

            RollupActionRequestValidationException validationException = new RollupActionRequestValidationException();
            if (dimensionFieldCaps.isEmpty()) {
                validationException.addValidationError("Index [" + sourceIndexName + "] does not contain any dimension fields");
            }
            if (metricFieldCaps.isEmpty()) {
                validationException.addValidationError("Index [" + sourceIndexName + "] does not contain any metric fields");
            }

            final XContentBuilder mapping;
            try {
                mapping = createRollupIndexMapping(request.getRollupConfig(), dimensionFieldCaps, metricFieldCaps);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            CreateIndexClusterStateUpdateRequest createIndexClusterStateUpdateRequest = new CreateIndexClusterStateUpdateRequest(
                "rollup",
                tmpIndexName,
                tmpIndexName
            ).settings(
                /*
                 * When creating the temporary rollup index, we copy the index.number_of_shards from source index,
                 * and we set the index.number_of_replicas to 0, to avoid replicating the temp index.
                 */
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, sourceIndexMetadata.getNumberOfShards())
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            ).mappings(XContentHelper.convertToJson(BytesReference.bytes(mapping), false, XContentType.JSON));

            // 2. Create hidden temporary rollup index
            clusterService.submitStateUpdateTask("create-rollup-index", new ClusterStateUpdateTask() {
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

                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    // 3. Temporary rollup index created. Run rollup indexer
                    RollupIndexerAction.Request rollupIndexerRequest = new RollupIndexerAction.Request(
                        tmpIndexName,
                        request,
                        dimensionFieldCaps.keySet().toArray(new String[0]),
                        metricFieldCaps.keySet().toArray(new String[0])
                    );

                    client.execute(RollupIndexerAction.INSTANCE, rollupIndexerRequest, ActionListener.wrap(indexerResp -> {
                        if (indexerResp.isCreated()) {
                            // 4. Make temp index read-only
                            UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(WRITE_BLOCKED_SETTINGS, tmpIndexName);
                            client.admin().indices().updateSettings(updateSettingsReq, ActionListener.wrap(updateSettingsResponse -> {
                                if (updateSettingsResponse.isAcknowledged()) {
                                    // 5. Clone final rollup index from the temporary rollup index
                                    ResizeRequest resizeRequest = new ResizeRequest(request.getRollupIndex(), tmpIndexName);
                                    resizeRequest.setResizeType(ResizeType.CLONE);
                                    /*
                                     * Clone will maintain the same index settings, including the number_of_shards
                                     * We must only copy the number_of_replicas from the source index
                                     */
                                    resizeRequest.getTargetIndexRequest()
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.SETTING_INDEX_HIDDEN, false)
                                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, sourceIndexMetadata.getNumberOfReplicas())
                                                .build()
                                        );
                                    client.admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(resizeResponse -> {
                                        if (resizeResponse.isAcknowledged()) {
                                            // 6. Publish rollup metadata and add rollup index to data stream
                                            publishMetadata(sourceIndexName, tmpIndexName, rollupIndexName, listener);
                                        } else {
                                            deleteTmpIndex(
                                                sourceIndexName,
                                                tmpIndexName,
                                                listener,
                                                new ElasticsearchException("Unable to resize temp rollup index [" + tmpIndexName + "]")
                                            );
                                        }
                                    }, e -> deleteTmpIndex(sourceIndexName, tmpIndexName, listener, e)));
                                } else {
                                    deleteTmpIndex(
                                        sourceIndexName,
                                        tmpIndexName,
                                        listener,
                                        new ElasticsearchException("Unable to update settings of temp rollup index [" + tmpIndexName + "]")
                                    );
                                }
                            }, e -> deleteTmpIndex(sourceIndexName, tmpIndexName, listener, e)));
                        } else {
                            deleteTmpIndex(
                                sourceIndexName,
                                tmpIndexName,
                                listener,
                                new ElasticsearchException("Unable to index into temp rollup index [" + tmpIndexName + "]")
                            );
                        }
                    }, e -> deleteTmpIndex(sourceIndexName, tmpIndexName, listener, e)));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }, newExecutor());
        }, listener::onFailure));
    }

    /**
     * Create a temporary index name for a rollup index by prefixing it with
     * the {@linkplain TransportRollupAction#TMP_ROLLUP_INDEX_PREFIX} prefix
     *
     * @param rollupIndexName the rollup index for which the temp index will be created
     */
    public static String createTmpIndexName(String rollupIndexName) {
        StringBuilder sb = new StringBuilder(TMP_ROLLUP_INDEX_PREFIX);
        if (rollupIndexName.startsWith(".")) {
            sb.append(rollupIndexName.substring(1));
        } else {
            sb.append(rollupIndexName);
        }
        return sb.toString();
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
    public static XContentBuilder createRollupIndexMapping(
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

        builder.endObject();
        return builder.endObject();
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

    private void publishMetadata(
        String sourceIndexName,
        String tmpIndexName,
        String rollupIndexName,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Update cluster state for the data stream to include the rollup index and exclude the source index
        clusterService.submitStateUpdateTask("update-rollup-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexAbstraction sourceIndex = currentState.getMetadata().getIndicesLookup().get(sourceIndexName);
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                if (sourceIndex.getParentDataStream() != null) {
                    IndexMetadata rollupIndexMetadata = currentState.getMetadata().index(rollupIndexName);
                    Index rollupIndex = rollupIndexMetadata.getIndex();
                    // If rolling up a backing index of a data stream, add rolled up index to backing data stream
                    DataStream originalDataStream = sourceIndex.getParentDataStream().getDataStream();
                    List<Index> backingIndices = new ArrayList<>(originalDataStream.getIndices().size());
                    // Adding the rollup index to the beginning of the list will prevent it from ever being
                    // considered a write index
                    backingIndices.add(rollupIndex);
                    // Add all indices except the source index
                    backingIndices.addAll(
                        originalDataStream.getIndices().stream().filter(idx -> idx.getName().equals(sourceIndexName) == false).toList()
                    );
                    DataStream dataStream = new DataStream(
                        originalDataStream.getName(),
                        backingIndices,
                        originalDataStream.getGeneration(),
                        originalDataStream.getMetadata(),
                        originalDataStream.isHidden(),
                        originalDataStream.isReplicated(),
                        originalDataStream.isSystem(),
                        originalDataStream.isAllowCustomRouting(),
                        originalDataStream.getIndexMode()
                    );
                    metadataBuilder.put(dataStream);
                }
                return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                // 7. Delete the source index
                deleteSourceIndex(sourceIndexName, tmpIndexName, listener);
            }

            @Override
            public void onFailure(Exception e) {
                deleteTmpIndex(
                    sourceIndexName,
                    tmpIndexName,
                    listener,
                    new ElasticsearchException("Failed to publish new cluster state with rollup metadata", e)
                );
            }
        }, newExecutor());
    }

    private void deleteSourceIndex(final String sourceIndex, final String tmpIndex, ActionListener<AcknowledgedResponse> listener) {
        client.admin().indices().delete(new DeleteIndexRequest(sourceIndex), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (acknowledgedResponse.isAcknowledged()) {
                    // Source index was deleted successfully.
                    // 8. Delete temporary rollup index
                    deleteTmpIndex(sourceIndex, tmpIndex, listener, null);
                } else {
                    onFailure(new ElasticsearchException("Failed to delete source index [" + sourceIndex + "]"));
                }
            }

            @Override
            public void onFailure(Exception deleteException) {
                deleteTmpIndex(
                    sourceIndex,
                    tmpIndex,
                    listener,
                    new ElasticsearchException("Failed to delete source index [" + sourceIndex + "].", deleteException)
                );
            }
        });
    }

    private void deleteTmpIndex(String sourceIndex, String tmpIndex, ActionListener<AcknowledgedResponse> listener, Exception e) {
        client.admin().indices().delete(new DeleteIndexRequest(tmpIndex), new ActionListener<>() {
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
                listener.onFailure(new ElasticsearchException("Unable to delete the temporary rollup index [" + tmpIndex + "]", e));
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }
}
