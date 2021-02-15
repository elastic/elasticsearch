/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
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
import org.elasticsearch.cluster.metadata.RollupGroup;
import org.elasticsearch.cluster.metadata.RollupMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupIndexerAction;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The master rollup action that coordinates
 *  -  creating rollup temporary index
 *  -  calling {@link TransportRollupIndexerAction} to index rolluped up documents
 *  -  cleaning up state
 */
public class TransportRollupAction extends AcknowledgedTransportMasterNodeAction<RollupAction.Request> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportRollupAction(Client client,
                                 ClusterService clusterService,
                                 TransportService transportService,
                                 ThreadPool threadPool,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(RollupAction.NAME, transportService, clusterService, threadPool, actionFilters, RollupAction.Request::new,
            indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.clusterService = clusterService;
    }

    @Override
    protected void masterOperation(Task task, RollupAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        String originalIndexName = request.getSourceIndex();
        String tmpIndexName = ".rolluptmp-" + request.getRollupIndex();

        final XContentBuilder mapping;
        try {
            mapping = getMapping(request.getRollupConfig());
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        CreateIndexRequest req = new CreateIndexRequest(tmpIndexName, Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build())
            .mapping(mapping);
        RollupIndexerAction.Request rollupIndexerRequest = new RollupIndexerAction.Request(request);
        ResizeRequest resizeRequest = new ResizeRequest(request.getRollupIndex(), tmpIndexName);
        resizeRequest.setResizeType(ResizeType.CLONE);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build());
        UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(
            Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build(), tmpIndexName);

        // 1. create hidden temporary index
        // 2. run rollup indexer
        // 3. make temp index read-only
        // 4. shrink index
        // 5. delete temporary index
        // at any point if there is an issue, then cleanup temp index
        client.admin().indices().create(req, ActionListener.wrap(createIndexResponse ->
            client.execute(RollupIndexerAction.INSTANCE, rollupIndexerRequest, ActionListener.wrap(indexerResp -> {
                if (indexerResp.isCreated()) {
                    client.admin().indices().updateSettings(updateSettingsReq, ActionListener.wrap(updateSettingsResponse -> {
                        if (updateSettingsResponse.isAcknowledged()) {
                            client.admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(resizeResponse -> {
                                if (resizeResponse.isAcknowledged()) {
                                    publishMetadata(request, originalIndexName, tmpIndexName, listener);
                                } else {
                                    deleteTmpIndex(originalIndexName, tmpIndexName, listener,
                                        new ElasticsearchException("Unable to resize temp rollup index [" + tmpIndexName + "]"));
                                }
                            }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                        } else {
                            deleteTmpIndex(originalIndexName, tmpIndexName, listener,
                                new ElasticsearchException("Unable to update settings of temp rollup index [" + tmpIndexName + "]"));
                        }
                    }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                } else {
                    deleteTmpIndex(originalIndexName, tmpIndexName, listener,
                        new ElasticsearchException("Unable to index into temp rollup index [" + tmpIndexName + "]"));
                }
            }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e))), listener::onFailure));
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
        String dateField = groupConfig.getDateHistogram().getField();
        HistogramGroupConfig histogramGroupConfig = groupConfig.getHistogram();
        List<MetricConfig> metricConfigs = config.getMetricsConfig();

        // TODO: Add the format of the original field
        builder.startObject(dateField).field("type", DateFieldMapper.CONTENT_TYPE).endObject();

        if (histogramGroupConfig != null) {
            for (String field : histogramGroupConfig.getFields()) {
                builder.startObject(field).field("type", NumberFieldMapper.NumberType.DOUBLE.typeName()).endObject();
            }
        }

        for (MetricConfig metricConfig : metricConfigs) {
            List<String> metrics = FieldMetricsProducer.normalizeMetrics(metricConfig.getMetrics());
            String defaultMetric = metrics.contains("value_count") ? "value_count" : metrics.get(0);
            builder.startObject(metricConfig.getField())
                .field("type", "aggregate_metric_double")
                .array("metrics", metrics.toArray())
                .field("default_metric", defaultMetric)
                .endObject();
        }

        return builder.endObject();
    }

    private void publishMetadata(RollupAction.Request request, String originalIndexName, String tmpIndexName,
                                 ActionListener<AcknowledgedResponse> listener) {
        // update Rollup metadata to include this index
        clusterService.submitStateUpdateTask("update-rollup-metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Everything went well, time to delete the temporary index
                deleteTmpIndex(originalIndexName, tmpIndexName, listener, null);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                String rollupIndexName = request.getRollupIndex();
                IndexMetadata rollupIndexMetadata = currentState.getMetadata().index(rollupIndexName);
                Index rollupIndex = rollupIndexMetadata.getIndex();
                // TODO(talevy): find better spot to get the original index name
                // extract created rollup index original index name to be used as metadata key
                String originalIndexName = request.getSourceIndex();
                Map<String, String> idxMetadata = currentState.getMetadata().index(originalIndexName)
                    .getCustomData(RollupMetadata.TYPE);
                String rollupGroupKeyName = (idxMetadata == null) ?
                    originalIndexName : idxMetadata.get(RollupMetadata.SOURCE_INDEX_NAME_META_FIELD);
                Map<String, String> rollupIndexRollupMetadata = new HashMap<>();
                rollupIndexRollupMetadata.put(RollupMetadata.SOURCE_INDEX_NAME_META_FIELD, rollupGroupKeyName);
                final RollupMetadata rollupMetadata = currentState.metadata().custom(RollupMetadata.TYPE);
                final Map<String, RollupGroup> rollupGroups;
                if (rollupMetadata == null) {
                    rollupGroups = new HashMap<>();
                } else {
                    rollupGroups = new HashMap<>(rollupMetadata.rollupGroups());
                }
                RollupActionDateHistogramGroupConfig dateConfig = request.getRollupConfig().getGroupConfig().getDateHistogram();
                WriteableZoneId rollupDateZoneId = WriteableZoneId.of(dateConfig.getTimeZone());
                if (rollupGroups.containsKey(rollupGroupKeyName)) {
                    RollupGroup group = rollupGroups.get(rollupGroupKeyName);
                    group.add(rollupIndexName, dateConfig.getInterval(), rollupDateZoneId);
                } else {
                    RollupGroup group = new RollupGroup();
                    group.add(rollupIndexName, dateConfig.getInterval(), rollupDateZoneId);
                    rollupGroups.put(rollupGroupKeyName, group);
                }
                // add rolled up index to backing datastream if rolling up a backing index of a datastream
                IndexAbstraction originalIndex = currentState.getMetadata().getIndicesLookup().get(originalIndexName);
                DataStream dataStream = null;
                if (originalIndex.getParentDataStream() != null) {
                    DataStream originalDataStream = originalIndex.getParentDataStream().getDataStream();
                    List<Index> backingIndices = new ArrayList<>(originalDataStream.getIndices());
                    backingIndices.add(rollupIndex);
                    dataStream = new DataStream(originalDataStream.getName(), originalDataStream.getTimeStampField(),
                        backingIndices, originalDataStream.getGeneration(), null);
                }
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata())
                    .put(IndexMetadata.builder(rollupIndexMetadata).putCustom(RollupMetadata.TYPE, rollupIndexRollupMetadata))
                    .putCustom(RollupMetadata.TYPE, new RollupMetadata(rollupGroups));
                if (dataStream != null) {
                    metadataBuilder.put(dataStream);
                }
                return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                deleteTmpIndex(originalIndexName, tmpIndexName,
                    listener, new ElasticsearchException("failed to publish new cluster state with rollup metadata", e));
            }
        });
    }

    private void deleteTmpIndex(String originalIndex, String tmpIndex, ActionListener<AcknowledgedResponse> listener, Exception e) {
        client.admin().indices().delete(new DeleteIndexRequest(tmpIndex), new ActionListener<>() {
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
                listener.onFailure(new ElasticsearchException("Unable to delete temp rollup index [" + tmpIndex  + "]", e));
            }
        });
    }
}
