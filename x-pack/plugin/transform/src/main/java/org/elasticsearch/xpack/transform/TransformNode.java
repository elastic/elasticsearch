/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Stateful representation of this node, relevant to the {@link org.elasticsearch.xpack.transform.transforms.TransformTask}.
 * For stateless functions, see {@link org.elasticsearch.xpack.transform.transforms.TransformNodes}.
 */
public class TransformNode {
    private final Supplier<Optional<ClusterState>> clusterState;
    private final Map<String, TransformTask> transformTasks = ConcurrentCollections.newConcurrentMap();
    private final List<Consumer<TransformTask>> deregistrationListeners = new CopyOnWriteArrayList<>();

    public TransformNode(Supplier<Optional<ClusterState>> clusterState) {
        this.clusterState = clusterState;
    }

    /**
     * Registers a transform task that started running on this node. Called alongside the
     * scheduler registration; {@link #deregisterTransform} is called wherever the scheduler
     * deregisters, so this map is bounded by the tasks currently running here.
     */
    public void registerTransform(TransformTask task) {
        transformTasks.put(task.getTransformId(), task);
    }

    /**
     * Removes the given transform's task and notifies the deregistration listeners with it.
     * No-op if the transform is not registered, so a task is reported once even if it is deregistered twice.
     */
    public void deregisterTransform(String transformId) {
        TransformTask removed = transformTasks.remove(transformId);
        if (removed != null) {
            deregistrationListeners.forEach(listener -> listener.accept(removed));
        }
    }

    /**
     * Subscribes to tasks leaving {@link #getTransformTasks()}. The listener runs on the deregistering thread, after the
     * task is removed and before its persistent task is completed, so it must be cheap and must not throw.
     */
    public void addDeregistrationListener(Consumer<TransformTask> listener) {
        deregistrationListeners.add(listener);
    }

    /**
     * Returns an unmodifiable, live view of the transform tasks currently running on this node.
     * Iteration is weakly consistent: safe from any thread and never throws
     * {@link java.util.ConcurrentModificationException}, but tasks registered or deregistered
     * mid-iteration may or may not be reflected.
     */
    public Collection<TransformTask> getTransformTasks() {
        return Collections.unmodifiableCollection(transformTasks.values());
    }

    /**
     * @return an optional containing true if this node is reported as shutting down in the cluster state metadata, false if it is not
     * reported as shutting down, or empty if the cluster state is missing or the local node has not been set yet.
     */
    public Optional<Boolean> isShuttingDown() {
        return clusterState.get().map(state -> {
            var localId = state.nodes().getLocalNodeId();
            if (localId != null) {
                return state.metadata().nodeShutdowns().contains(localId);
            } else {
                return null; // empty
            }
        });
    }

    /**
     * @return the node id stored in the cluster state, or "null" if the cluster state is missing or the local node has not been set yet.
     * This should behave exactly as {@link String#valueOf(Object)}.
     */
    public String nodeId() {
        return clusterState.get().map(ClusterState::nodes).map(DiscoveryNodes::getLocalNodeId).orElse("null");
    }
}
