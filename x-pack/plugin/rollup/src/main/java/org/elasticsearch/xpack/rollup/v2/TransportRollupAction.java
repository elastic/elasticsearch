/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RollupGroup;
import org.elasticsearch.cluster.metadata.RollupMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupTask;
import org.elasticsearch.xpack.rollup.Rollup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO(talevy): enforce that rollup-indices of indices backing a datastream must be hidden
public class TransportRollupAction extends HandledTransportAction<RollupAction.Request, RollupAction.Response> {
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    @Inject
    public TransportRollupAction(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final TransportService transportService,
            final ActionFilters actionFilters
    ) {
        super(RollupAction.NAME, transportService, actionFilters, RollupAction.Request::new);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, RollupAction.Request request, ActionListener<RollupAction.Response> listener) {
        RollupTask rollupTask = (RollupTask) task;
        RollupV2Indexer indexer = new RollupV2Indexer(client, threadPool, Rollup.TASK_THREAD_POOL_NAME,
            rollupTask.config(), rollupTask.headers(), ActionListener.wrap(c -> {
            // update Rollup metadata to include this index
            clusterService.submitStateUpdateTask("update-rollup-metadata", new ClusterStateUpdateTask() {

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new RollupAction.Response(true));
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    String rollupIndexName = rollupTask.config().getRollupIndex();
                    IndexMetadata rollupIndexMetadata = currentState.getMetadata().index(rollupIndexName);
                    Index rollupIndex = rollupIndexMetadata.getIndex();
                    // TODO(talevy): find better spot to get the original index name
                    // extract created rollup index original index name to be used as metadata key
                    String originalIndexName = rollupTask.config().getSourceIndex();
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
                    DateHistogramGroupConfig dateConfig = rollupTask.config().getGroupConfig().getDateHistogram();
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
                        backingIndices.add(backingIndices.size() - 1, rollupIndex);
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
                    listener.onFailure(new ElasticsearchException("failed to publish new cluster state with rollup metadata", e));
                }
            });
        }, e -> listener.onFailure(
            new ElasticsearchException("Failed to rollup index [" + rollupTask.config().getSourceIndex() + "]", e))));
        if (indexer.start() == IndexerState.STARTED) {
            indexer.maybeTriggerAsyncJob(Long.MAX_VALUE);
        } else {
            listener.onFailure(new ElasticsearchException("failed to start rollup task"));
        }
    }
}
