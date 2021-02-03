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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.xpack.rollup.Rollup.TASK_THREAD_POOL_NAME;

/**
 * A {@link TransportBroadcastAction} that rollups all the shards of a single index into a new one.
 *
 * TODO: Enforce that rollup-indices of indices backing a datastream must be hidden.
 *       Enforce that we don't retry on another replica if we throw an error after sending some buckets.
 */
public class TransportRollupAction
    extends TransportBroadcastAction<RollupAction.Request, RollupAction.Response, RollupAction.ShardRequest, RollupAction.ShardResponse> {

    private static final int SORTER_RAM_SIZE_MB = 100;

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportRollupAction(Client client,
                                 ClusterService clusterService,
                                 TransportService transportService,
                                 IndicesService indicesService,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(RollupAction.NAME, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, RollupAction.Request::new, RollupAction.ShardRequest::new, TASK_THREAD_POOL_NAME);
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState, RollupAction.Request request, String[] concreteIndices) {
        if (concreteIndices.length > 1) {
            throw new IllegalArgumentException("multiple indices: " + Arrays.toString(concreteIndices));
        }
        // Random routing to limit request to a single shard
        String routing = Integer.toString(Randomness.get().nextInt(1000));
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, routing, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RollupAction.Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RollupAction.Request request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    @Override
    protected void doExecute(Task task, RollupAction.Request request, ActionListener<RollupAction.Response> listener) {
        try {
            String tmpIndexName =  ".rolluptmp-" + request.getRollupIndex();
            createTempRollupIndex(request, tmpIndexName,
                ActionListener.wrap(
                    resp -> {
                        new Async(task, request, listener).start();
                    },
                    listener::onFailure
                )
            );
        } catch (IOException exc) {
            // here because the mapping could not be parsed. temp index was not created.
            listener.onFailure(exc);
        }
    }

    @Override
    protected RollupAction.ShardRequest newShardRequest(int numShards, ShardRouting shard, RollupAction.Request request) {
        return new RollupAction.ShardRequest(shard.shardId(), request);
    }

    @Override
    protected RollupAction.ShardResponse shardOperation(RollupAction.ShardRequest request, Task task) throws IOException {
        IndexService indexService = indicesService.indexService(request.shardId().getIndex());
        String tmpIndexName =  ".rolluptmp-" + request.getRollupIndex();
        RollupShardIndexer indexer = new RollupShardIndexer(client, indexService, request.shardId(),
            request.getRollupConfig(), tmpIndexName, SORTER_RAM_SIZE_MB);
        indexer.execute();
        return new RollupAction.ShardResponse(request.shardId());
    }

    @Override
    protected RollupAction.ShardResponse readShardResponse(StreamInput in) throws IOException {
        return new RollupAction.ShardResponse(in);
    }

    @Override
    protected RollupAction.Response newResponse(RollupAction.Request request,
                                                AtomicReferenceArray<?> shardsResponses,
                                                ClusterState clusterState) {
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                throw new ElasticsearchException("missing shard");
            } else if (shardResponse instanceof Exception) {
                throw new ElasticsearchException((Exception) shardResponse);
            }
        }
        return new RollupAction.Response(true);
    }

    private class Async extends AsyncBroadcastAction {
        private final RollupAction.Request request;
        private final ActionListener<RollupAction.Response> listener;

        protected Async(Task task, RollupAction.Request request, ActionListener<RollupAction.Response> listener) {
            super(task, request, listener);
            this.request = request;
            this.listener = listener;
        }

        @Override
        protected void finishHim() {
            try {
                RollupAction.Response resp = newResponse(request, shardsResponses, clusterService.state());
                shrinkAndPublishIndex(request, ActionListener.wrap(v -> listener.onResponse(resp), listener::onFailure));
            } catch (Exception e) {
                deleteTmpIndex(".rolluptmp-" + request.getRollupIndex(), ActionListener.wrap(() -> listener.onFailure(e)));
            }
        }
    }

    private void createTempRollupIndex(RollupAction.Request request,
                                       String tmpIndex,
                                       ActionListener<CreateIndexResponse> listener) throws IOException {
        CreateIndexRequest req = new CreateIndexRequest(tmpIndex, Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build())
            .mapping(getMapping(request.getRollupConfig()));
        client.admin().indices().create(req, listener);
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

    private void shrinkAndPublishIndex(RollupAction.Request request, ActionListener<RollupAction.Response> listener) {
        String tmpIndexName = ".rolluptmp-" + request.getRollupIndex();
        ResizeRequest resizeRequest = new ResizeRequest(request.getRollupIndex(), tmpIndexName);
        resizeRequest.setResizeType(ResizeType.CLONE);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build());
        UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(
            Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build(), tmpIndexName);
        client.admin().indices().updateSettings(updateSettingsReq,
            ActionListener.wrap(
                resp -> {
                    client.admin().indices().resizeIndex(resizeRequest,
                        ActionListener.wrap(ack -> publishMetadata(request, listener), listener::onFailure));
                },
                listener::onFailure
            )
        );
    }

    private void publishMetadata(RollupAction.Request request, ActionListener<RollupAction.Response> listener) {
        // update Rollup metadata to include this index
        clusterService.submitStateUpdateTask("update-rollup-metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Everything went well, time to delete the temporary index
                deleteTmpIndex(".rolluptmp-" + request.getRollupIndex(),
                    ActionListener.wrap(r -> listener.onResponse(new RollupAction.Response(true)), listener::onFailure));
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
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
                // Everything went well, time to delete the temporary index
                deleteTmpIndex(".rolluptmp-" + request.getRollupIndex(),
                    ActionListener.wrap(() -> listener.onFailure(
                        new ElasticsearchException("failed to publish new cluster state with rollup metadata", e))));
            }
        });
    }

    private void deleteTmpIndex(String tmpIndex, ActionListener<AcknowledgedResponse> listener) {
        client.admin().indices().delete(new DeleteIndexRequest(tmpIndex), listener);
    }
}
