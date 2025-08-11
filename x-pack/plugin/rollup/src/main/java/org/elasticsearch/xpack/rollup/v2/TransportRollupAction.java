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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.action.RollupActionRequestValidationException;
import org.elasticsearch.xpack.core.rollup.action.RollupIndexerAction;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The master rollup action that coordinates
 *  -  creating rollup temporary index
 *  -  calling {@link TransportRollupIndexerAction} to index rolluped up documents
 *  -  cleaning up state
 */
public class TransportRollupAction extends AcknowledgedTransportMasterNodeAction<RollupAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportRollupAction.class);

    private static final Settings VISIBLE_INDEX_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build();
    private static final Settings WRITE_BLOCKED_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();

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
    protected void masterOperation(RollupAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        logger.warn("attempt to execute a rollup action without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void masterOperation(
        Task task,
        RollupAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws IOException {
        String originalIndexName = request.getSourceIndex();

        final String rollupIndexName;
        if (request.getRollupIndex() == null) {
            rollupIndexName = "rollup-" + originalIndexName + "-" + UUIDs.randomBase64UUID(Randomness.get());
        } else {
            rollupIndexName = request.getRollupIndex();
        }

        String tmpIndexName = ".rolluptmp-" + rollupIndexName;

        final XContentBuilder mapping;
        try {
            mapping = getMapping(request.getRollupConfig());
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices(originalIndexName)
            .fields(request.getRollupConfig().getAllFields().toArray(new String[0]));
        fieldCapsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        // Add the source index name and UUID to the rollup index metadata. If the original index is a rollup index itself,
        // we will add the name and UUID of the raw index that we initially rolled up.
        IndexMetadata originalIndexMetadata = state.getMetadata().index(originalIndexName);
        String sourceIndexName = IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.exists(originalIndexMetadata.getSettings())
            ? IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.get(originalIndexMetadata.getSettings())
            : originalIndexName;
        String sourceIndexUuid = IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.exists(originalIndexMetadata.getSettings())
            ? IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.get(originalIndexMetadata.getSettings())
            : originalIndexMetadata.getIndexUUID();

        // Using the CreateIndexRequest class only to produce the transformation to the mappings map.
        // Alternatively, we would have to copy-paste that part of the code.
        Map<String, String> mappingAsMap = new CreateIndexRequest().mapping("_doc", mapping).mappings();
        CreateIndexClusterStateUpdateRequest createIndexClusterStateUpdateRequest = new CreateIndexClusterStateUpdateRequest(
            "rollup",
            tmpIndexName,
            tmpIndexName
        ).settings(MetadataRolloverService.HIDDEN_INDEX_SETTINGS).mappings(mappingAsMap);

        RollupIndexerAction.Request rollupIndexerRequest = new RollupIndexerAction.Request(request);
        ResizeRequest resizeRequest = new ResizeRequest(request.getRollupIndex(), tmpIndexName);
        resizeRequest.setResizeType(ResizeType.CLONE);
        resizeRequest.getTargetIndexRequest().settings(VISIBLE_INDEX_SETTINGS);
        UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(WRITE_BLOCKED_SETTINGS, tmpIndexName);

        // 1. validate Rollup Config against Field Caps
        // 2. create hidden temporary index
        // 3. run rollup indexer
        // 4. make temp index read-only
        // 5. shrink index
        // 6. delete temporary index
        // at any point if there is an issue, then cleanup temp index

        client.fieldCaps(fieldCapsRequest, ActionListener.wrap(fieldCapsResponse -> {
            RollupActionRequestValidationException validationException = new RollupActionRequestValidationException();
            if (fieldCapsResponse.get().size() == 0) {
                validationException.addValidationError(
                    "Could not find any fields in the index [" + originalIndexName + "] that were configured in job"
                );
                listener.onFailure(validationException);
                return;
            }
            request.getRollupConfig().validateMappings(fieldCapsResponse.get(), validationException);
            if (validationException.validationErrors().size() > 0) {
                listener.onFailure(validationException);
                return;
            }

            clusterService.submitStateUpdateTask("rollup create index", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return metadataCreateIndexService.applyCreateIndexRequest(
                        currentState,
                        createIndexClusterStateUpdateRequest,
                        true,
                        (builder, indexMetadata) -> builder.put(
                            IndexMetadata.builder(indexMetadata)
                                .settings(
                                    Settings.builder()
                                        .put(indexMetadata.getSettings())
                                        .put(IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.getKey(), sourceIndexName)
                                        .put(IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.getKey(), sourceIndexUuid)
                                )
                        )
                    );
                }

                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    // index created
                    client.execute(RollupIndexerAction.INSTANCE, rollupIndexerRequest, ActionListener.wrap(indexerResp -> {
                        if (indexerResp.isCreated()) {
                            client.admin().indices().updateSettings(updateSettingsReq, ActionListener.wrap(updateSettingsResponse -> {
                                if (updateSettingsResponse.isAcknowledged()) {
                                    client.admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(resizeResponse -> {
                                        if (resizeResponse.isAcknowledged()) {
                                            publishMetadata(originalIndexName, tmpIndexName, rollupIndexName, listener);
                                        } else {
                                            deleteTmpIndex(
                                                originalIndexName,
                                                tmpIndexName,
                                                listener,
                                                new ElasticsearchException("Unable to resize temp rollup index [" + tmpIndexName + "]")
                                            );
                                        }
                                    }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                                } else {
                                    deleteTmpIndex(
                                        originalIndexName,
                                        tmpIndexName,
                                        listener,
                                        new ElasticsearchException("Unable to update settings of temp rollup index [" + tmpIndexName + "]")
                                    );
                                }
                            }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                        } else {
                            deleteTmpIndex(
                                originalIndexName,
                                tmpIndexName,
                                listener,
                                new ElasticsearchException("Unable to index into temp rollup index [" + tmpIndexName + "]")
                            );
                        }
                    }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            });
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(RollupAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private XContentBuilder getMapping(RollupActionConfig config) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder = getDynamicTemplates(builder);
        builder = getProperties(builder, config);
        return builder.endObject();
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

    /**
     * Creates the rollup mapping properties from the provided {@link RollupActionConfig}.
     */
    private static XContentBuilder getProperties(XContentBuilder builder, RollupActionConfig config) throws IOException {
        builder.startObject("properties");

        RollupActionGroupConfig groupConfig = config.getGroupConfig();
        RollupActionDateHistogramGroupConfig dateHistogramConfig = groupConfig.getDateHistogram();
        String dateField = dateHistogramConfig.getField();
        String dateIntervalType = dateHistogramConfig.getIntervalTypeName();
        String dateInterval = dateHistogramConfig.getInterval().toString();
        String tz = dateHistogramConfig.getTimeZone() != null
            ? dateHistogramConfig.getTimeZone()
            : RollupActionDateHistogramGroupConfig.DEFAULT_TIMEZONE;

        builder.startObject(dateField)
            .field("type", DateFieldMapper.CONTENT_TYPE)
            .startObject("meta")
            .field(dateIntervalType, dateInterval)
            .field(RollupActionDateHistogramGroupConfig.CalendarInterval.TIME_ZONE, tz)
            .endObject()
            .endObject();

        HistogramGroupConfig histogramGroupConfig = groupConfig.getHistogram();
        if (histogramGroupConfig != null) {
            for (String field : histogramGroupConfig.getFields()) {
                builder.startObject(field)
                    .field("type", NumberFieldMapper.NumberType.DOUBLE.typeName())
                    .startObject("meta")
                    .field(HistogramGroupConfig.INTERVAL, String.valueOf(histogramGroupConfig.getInterval()))
                    .endObject()
                    .endObject();
            }
        }

        List<MetricConfig> metricConfigs = config.getMetricsConfig();
        for (MetricConfig metricConfig : metricConfigs) {
            List<String> metrics = FieldMetricsProducer.normalizeMetrics(metricConfig.getMetrics());
            String defaultMetric = metrics.contains("value_count") ? "value_count" : metrics.get(0);
            builder.startObject(metricConfig.getField())
                .field("type", AggregateDoubleMetricFieldMapper.CONTENT_TYPE)
                .stringListField(AggregateDoubleMetricFieldMapper.Names.METRICS, metrics)
                .field(AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC, defaultMetric)
                .endObject();
        }

        return builder.endObject();
    }

    private void publishMetadata(
        String originalIndexName,
        String tmpIndexName,
        String rollupIndexName,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Update rollup metadata to include this index
        clusterService.submitStateUpdateTask("update-rollup-metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Everything went well, time to delete the temporary index
                deleteTmpIndex(originalIndexName, tmpIndexName, listener, null);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata rollupIndexMetadata = currentState.getMetadata().index(rollupIndexName);
                Index rollupIndex = rollupIndexMetadata.getIndex();
                IndexAbstraction originalIndex = currentState.getMetadata().getIndicesLookup().get(originalIndexName);

                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                if (originalIndex.getParentDataStream() != null) {
                    // If rolling up a backing index of a data stream, add rolled up index to backing data stream
                    DataStream originalDataStream = originalIndex.getParentDataStream().getDataStream();
                    List<Index> backingIndices = new ArrayList<>(originalDataStream.getIndices().size() + 1);
                    // Adding rollup indices to the beginning of the list will prevent rollup indices from ever being
                    // considered a write index
                    backingIndices.add(rollupIndex);
                    backingIndices.addAll(originalDataStream.getIndices());
                    DataStream dataStream = new DataStream(
                        originalDataStream.getName(),
                        originalDataStream.getTimeStampField(),
                        backingIndices,
                        originalDataStream.getGeneration(),
                        originalDataStream.getMetadata(),
                        originalDataStream.isHidden(),
                        originalDataStream.isReplicated(),
                        originalDataStream.isSystem()
                    );
                    metadataBuilder.put(dataStream);
                }
                return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                deleteTmpIndex(
                    originalIndexName,
                    tmpIndexName,
                    listener,
                    new ElasticsearchException("failed to publish new cluster state with rollup metadata", e)
                );
            }
        });
    }

    private void deleteTmpIndex(String originalIndex, String tmpIndex, ActionListener<AcknowledgedResponse> listener, Exception e) {
        client.admin().indices().delete(new DeleteIndexRequest(tmpIndex), new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (e == null && acknowledgedResponse.isAcknowledged()) {
                    listener.onResponse(acknowledgedResponse);
                } else {
                    listener.onFailure(new ElasticsearchException("Unable to rollup index [" + originalIndex + "]", e));
                }
            }

            @Override
            public void onFailure(Exception deleteException) {
                listener.onFailure(new ElasticsearchException("Unable to delete temp rollup index [" + tmpIndex + "]", e));
            }
        });
    }
}
