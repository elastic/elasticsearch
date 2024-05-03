/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.action.DeleteDataStreamGlobalRetentionAction;
import org.elasticsearch.datastreams.lifecycle.action.PutDataStreamGlobalRetentionAction;
import org.elasticsearch.datastreams.lifecycle.action.UpdateDataStreamGlobalRetentionResponse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * This service manages the global retention configuration, it provides an API to set or remove global retention
 * from the cluster state.
 */
public class UpdateDataStreamGlobalRetentionService {

    private static final Logger logger = LogManager.getLogger(UpdateDataStreamGlobalRetentionService.class);

    private final DataStreamGlobalRetentionResolver globalRetentionResolver;
    private final MasterServiceTaskQueue<UpsertGlobalDataStreamMetadataTask> taskQueue;

    public UpdateDataStreamGlobalRetentionService(
        ClusterService clusterService,
        DataStreamGlobalRetentionResolver globalRetentionResolver
    ) {
        this.globalRetentionResolver = globalRetentionResolver;
        ClusterStateTaskExecutor<UpsertGlobalDataStreamMetadataTask> executor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpsertGlobalDataStreamMetadataTask task,
                ClusterState clusterState
            ) {
                return new Tuple<>(updateGlobalRetention(clusterState, task.globalRetention()), task);
            }
        };
        this.taskQueue = clusterService.createTaskQueue("data-stream-global-retention", Priority.HIGH, executor);

    }

    public void updateGlobalRetention(
        PutDataStreamGlobalRetentionAction.Request request,
        List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams,
        final ActionListener<UpdateDataStreamGlobalRetentionResponse> listener
    ) {
        taskQueue.submitTask(
            "update-data-stream-global-retention",
            new UpsertGlobalDataStreamMetadataTask(
                request.getGlobalRetention(),
                affectedDataStreams,
                listener,
                request.masterNodeTimeout()
            ),
            request.masterNodeTimeout()
        );
    }

    public void removeGlobalRetention(
        DeleteDataStreamGlobalRetentionAction.Request request,
        List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams,
        final ActionListener<UpdateDataStreamGlobalRetentionResponse> listener
    ) {
        taskQueue.submitTask(
            "remove-data-stream-global-retention",
            new UpsertGlobalDataStreamMetadataTask(null, affectedDataStreams, listener, request.masterNodeTimeout()),
            request.masterNodeTimeout()
        );
    }

    public List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> determineAffectedDataStreams(
        @Nullable DataStreamGlobalRetention newGlobalRetention,
        ClusterState clusterState
    ) {
        var previousGlobalRetention = globalRetentionResolver.resolve(clusterState);
        if (Objects.equals(newGlobalRetention, previousGlobalRetention)) {
            return List.of();
        }
        List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams = new ArrayList<>();
        for (DataStream dataStream : clusterState.metadata().dataStreams().values()) {
            if (dataStream.getLifecycle() != null) {
                TimeValue previousEffectiveRetention = dataStream.getLifecycle()
                    .getEffectiveDataRetention(dataStream.isSystem() ? null : previousGlobalRetention);
                TimeValue newEffectiveRetention = dataStream.getLifecycle()
                    .getEffectiveDataRetention(dataStream.isSystem() ? null : newGlobalRetention);
                if (Objects.equals(previousEffectiveRetention, newEffectiveRetention) == false) {
                    affectedDataStreams.add(
                        new UpdateDataStreamGlobalRetentionResponse.AffectedDataStream(
                            dataStream.getName(),
                            newEffectiveRetention,
                            previousEffectiveRetention
                        )
                    );
                }
            }
        }
        affectedDataStreams.sort(Comparator.comparing(UpdateDataStreamGlobalRetentionResponse.AffectedDataStream::dataStreamName));
        return affectedDataStreams;
    }

    // Visible for testing
    ClusterState updateGlobalRetention(ClusterState clusterState, @Nullable DataStreamGlobalRetention retentionFromRequest) {
        // Detecting if this update will result in a change in the cluster state, requires to use only the global retention from
        // the cluster state and not the factory retention.
        final var initialRetentionFromClusterState = DataStreamGlobalRetention.getFromClusterState(clusterState);
        // Avoid storing empty retention in the cluster state
        final var newRetention = DataStreamGlobalRetention.EMPTY.equals(retentionFromRequest) ? null : retentionFromRequest;
        if (Objects.equals(newRetention, initialRetentionFromClusterState)) {
            return clusterState;
        }
        if (newRetention == null) {
            return clusterState.copyAndUpdate(b -> b.removeCustom(DataStreamGlobalRetention.TYPE));
        }
        return clusterState.copyAndUpdate(b -> b.putCustom(DataStreamGlobalRetention.TYPE, newRetention));
    }

    /**
     * A base class for the task updating the global retention in the cluster state.
     */
    record UpsertGlobalDataStreamMetadataTask(
        @Nullable DataStreamGlobalRetention globalRetention,
        List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams,
        ActionListener<UpdateDataStreamGlobalRetentionResponse> listener,
        TimeValue ackTimeout
    ) implements ClusterStateTaskListener, ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.onResponse(new UpdateDataStreamGlobalRetentionResponse(true, affectedDataStreams));
        }

        @Override
        public void onAckFailure(Exception e) {
            logger.debug("Failed to update global retention [{}] with error [{}]", globalRetention, e.getMessage());
            listener.onResponse(UpdateDataStreamGlobalRetentionResponse.FAILED);
        }

        @Override
        public void onAckTimeout() {
            logger.debug("Failed to update global retention [{}] because timeout was reached", globalRetention);
            listener.onResponse(UpdateDataStreamGlobalRetentionResponse.FAILED);
        }
    }
}
