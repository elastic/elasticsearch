/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
     * Called to determine which nodes the acknowledgement is expected from.
     *
     * As this method will be called multiple times to determine the set of acking nodes,
     * it is crucial for it to return consistent results: Given the same listener instance
     * and the same node parameter, the method implementation should return the same result.
     *
     * @param discoveryNode a node
     * @return true if the node is expected to send ack back, false otherwise
     */
    boolean mustAck(DiscoveryNode discoveryNode);

    /**
     * Called once all the nodes have acknowledged the cluster state update request. Must be
     * very lightweight execution, since it gets executed on the cluster service thread.
     */
    void onAllNodesAcked();

    /**
     * Called after all the nodes have acknowledged the cluster state update request but at least one of them failed. Must be
     * very lightweight execution, since it gets executed on the cluster service thread.
     *
     * @param e optional error that might have been thrown
     */
    void onAckFailure(Exception e);

    /**
     * Called once the acknowledgement timeout defined by
     * {@link AckedClusterStateUpdateTask#ackTimeout()} has expired
     */
    void onAckTimeout();

    /**
     * @return acknowledgement timeout, maximum time interval to wait for acknowledgements
     */
    TimeValue ackTimeout();

}
