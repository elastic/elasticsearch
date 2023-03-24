/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;

import java.util.List;

/**
 * This service modifies the data lifecycle of an existing data stream. It creates and batches the cluster state update tasks
 * that will change the data stream metadata in the cluster state.
 */
public class ModifyDataLifecycleService {

    private final MasterServiceTaskQueue<ModifyLifecycleTask> taskQueue;

    @Inject
    public ModifyDataLifecycleService(ClusterService clusterService) {
        ClusterStateTaskExecutor<ModifyLifecycleTask> executor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                ModifyLifecycleTask modifyLifecycleTask,
                ClusterState clusterState
            ) {
                return new Tuple<>(
                    modifyLifecycle(clusterState, modifyLifecycleTask.dataStreamNames(), modifyLifecycleTask.dataLifecycle()),
                    modifyLifecycleTask
                );
            }
        };
        this.taskQueue = clusterService.createTaskQueue("modify-lifecycle", Priority.LOW, executor);
    }

    /**
     * Submits the task to set the lifecycle to the requested data streams.
     */
    public void setLifecycle(
        final List<String> dataStreamNames,
        DataLifecycle lifecycle,
        TimeValue ackTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        // TODO use request.masterNodeTimeout() as timeout?
        taskQueue.submitTask("set-lifecycle", new ModifyLifecycleTask(dataStreamNames, lifecycle, ackTimeout, listener), null);
    }

    /**
     * Submits the task to remove the lifecycle from the requested data streams.
     */
    public void removeLifecycle(List<String> dataStreamNames, TimeValue ackTimeout, ActionListener<AcknowledgedResponse> listener) {
        // TODO use request.masterNodeTimeout() as timeout?
        taskQueue.submitTask("delete-lifecycle", new ModifyLifecycleTask(dataStreamNames, null, ackTimeout, listener), null);
    }

    /**
     * Creates an updated cluster state in which the requested data streams have the data lifecycle provided.
     * Visible for testing.
     */
    ClusterState modifyLifecycle(ClusterState currentState, List<String> dataStreamNames, @Nullable DataLifecycle dataLifecycle) {
        Metadata updatedMetadata = currentState.metadata();
        Metadata.Builder builder = Metadata.builder(updatedMetadata);
        for (var dataStreamName : dataStreamNames) {
            var dataStream = validateDataStream(updatedMetadata, dataStreamName);
            builder.put(
                new DataStream(
                    dataStream.getName(),
                    dataStream.getIndices(),
                    dataStream.getGeneration(),
                    dataStream.getMetadata(),
                    dataStream.isHidden(),
                    dataStream.isReplicated(),
                    dataStream.isSystem(),
                    dataStream.isAllowCustomRouting(),
                    dataStream.getIndexMode(),
                    dataLifecycle
                )
            );
        }
        updatedMetadata = builder.build();
        return ClusterState.builder(currentState).metadata(updatedMetadata).build();
    }

    private static DataStream validateDataStream(Metadata metadata, String dataStreamName) {
        IndexAbstraction dataStream = metadata.getIndicesLookup().get(dataStreamName);
        if (dataStream == null || dataStream.getType() != IndexAbstraction.Type.DATA_STREAM) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return (DataStream) dataStream;
    }

    /**
     * A cluster state update task that consists of the cluster state request and the listeners that need to be notified upon completion.
     */
    record ModifyLifecycleTask(
        List<String> dataStreamNames,
        @Nullable DataLifecycle dataLifecycle,
        TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
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
            listener.onResponse(AcknowledgedResponse.TRUE);
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }
    }
}
