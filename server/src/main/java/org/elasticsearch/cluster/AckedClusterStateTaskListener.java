/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

public interface AckedClusterStateTaskListener extends ClusterStateTaskListener {

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
     *
     * @param e optional error that might have been thrown
     */
    void onAllNodesAcked(@Nullable Exception e);

    /**
     * Called once the acknowledgement timeout defined by
     * {@link AckedClusterStateUpdateTask#ackTimeout()} has expired
     */
    void onAckTimeout();

    /**
     * Acknowledgement timeout, maximum time interval to wait for acknowledgements
     */
    TimeValue ackTimeout();

}
