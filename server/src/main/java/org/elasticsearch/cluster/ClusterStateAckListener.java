/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;

/**
 * Interface that a cluster state update task can implement to indicate that it wishes to be notified when the update has been acked by
 * (some subset of) the nodes in the cluster. Nodes ack a cluster state update after successfully applying the resulting state. Note that
 * updates which do not change the cluster state are automatically reported as acked by all nodes without checking to see whether there are
 * any nodes that have not already applied this state.
 */
public interface ClusterStateAckListener {

    /**
     * Called to determine the nodes from which an acknowledgement is expected.
     * <p>
     * This method will be called multiple times to determine the set of acking nodes, so it is crucial for it to return consistent results:
     * Given the same listener instance and the same node parameter, the method implementation should return the same result.
     *
     * @return {@code true} if and only if this task will wait for an ack from the given node.
     */
    boolean mustAck(DiscoveryNode discoveryNode);

    /**
     * Called once all the selected nodes have acknowledged the cluster state update request. Must be very lightweight execution, since it
     * is executed on the cluster service thread.
     */
    void onAllNodesAcked();

    /**
     * Called after all the nodes have acknowledged the cluster state update request but at least one of them failed. Must be very
     * lightweight execution, since it is executed on the cluster service thread.
     *
     * @param e exception representing the failure.
     */
    void onAckFailure(Exception e);

    /**
     * Called if the acknowledgement timeout defined by {@link ClusterStateAckListener#ackTimeout()} expires while still waiting for acks.
     */
    void onAckTimeout();

    /**
     * @return acknowledgement timeout, i.e. the maximum time interval to wait for acknowledgements. Return {@link TimeValue#MINUS_ONE} if
     *         the request should wait indefinitely for acknowledgements.
     */
    TimeValue ackTimeout();

}
