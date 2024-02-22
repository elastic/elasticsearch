/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
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

public class UpdateDataStreamGlobalRetentionService {

    private final MasterServiceTaskQueue<UpsertGlobalDataStreamMetadataTask> taskQueue;

    public UpdateDataStreamGlobalRetentionService(ClusterService clusterService) {
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
        var previousGlobalRetention = DataStreamGlobalRetention.getFromClusterState(clusterState);
        if (Objects.equals(newGlobalRetention, previousGlobalRetention)) {
            return List.of();
        }
        List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams = new ArrayList<>();
        for (DataStream dataStream : clusterState.metadata().dataStreams().values()) {
            if (dataStream.getLifecycle() != null) {
                TimeValue previousEffectiveRetention = dataStream.getLifecycle().getEffectiveDataRetention(previousGlobalRetention);
                TimeValue newEffectiveRetention = dataStream.getLifecycle().getEffectiveDataRetention(newGlobalRetention);
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
        final var initialRetention = DataStreamGlobalRetention.getFromClusterState(clusterState);
        // Avoid storing empty retention in the cluster state
        final var newRetention = DataStreamGlobalRetention.EMPTY.equals(retentionFromRequest) ? null : retentionFromRequest;
        if (Objects.equals(newRetention, initialRetention)) {
            return clusterState;
        }
        if (newRetention == null) {
            return clusterState.copyAndUpdate(b -> b.removeCustom(DataStreamGlobalRetention.TYPE));
        }
        return clusterState.copyAndUpdate(b -> b.putCustom(DataStreamGlobalRetention.TYPE, newRetention));
    }

    /**
     * A base class for health metadata cluster state update tasks.
     */
    record UpsertGlobalDataStreamMetadataTask(
        @Nullable DataStreamGlobalRetention globalRetention,
        List<UpdateDataStreamGlobalRetentionResponse.AffectedDataStream> affectedDataStreams,
        ActionListener<UpdateDataStreamGlobalRetentionResponse> listener,
        TimeValue masterTimeout
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
            listener.onResponse(new UpdateDataStreamGlobalRetentionResponse(false));
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(new UpdateDataStreamGlobalRetentionResponse(false));
        }

        @Override
        public TimeValue ackTimeout() {
            return masterTimeout;
        }
    }
}
