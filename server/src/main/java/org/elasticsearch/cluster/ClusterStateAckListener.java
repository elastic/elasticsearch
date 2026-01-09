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
     * Called after all the selected nodes have acknowledged the cluster state update request but at least one of them failed. Must be very
     * lightweight execution, since it is executed on the cluster service thread.
     *
     * @param e exception representing the failure.
     */
    void onAckFailure(Exception e);

    /**
     * Called if the acknowledgement timeout defined by {@link #ackTimeout()} expires while still waiting for an acknowledgement from one
     * or more of the selected nodes.
     */
    void onAckTimeout();

    /**
     * @return acknowledgement timeout, i.e. the maximum time interval to wait for a full set of acknowledgements. This time interval is
     *         measured from the start of the publication (which is after computing the new cluster state and serializing it as a transport
     *         message). If the cluster state is committed (i.e. a quorum of master-eligible nodes have accepted the new state) and then the
     *         timeout elapses then the corresponding listener is completed via {@link
     *         org.elasticsearch.cluster.ClusterStateAckListener#onAckTimeout()}. Although the time interval is measured from the start of
     *         the publication, it does not have any effect until the cluster state is committed:
     *         <ul>
     *             <li>
     *                If the cluster state update fails before committing then the failure is always reported via {@link
     *                org.elasticsearch.cluster.ClusterStateAckListener#onAckFailure(Exception)} rather than {@link
     *                org.elasticsearch.cluster.ClusterStateAckListener#onAckTimeout()}, and this may therefore happen some time after the
     *                timeout period elapses.
     *             </li>
     *             <li>
     *                If the cluster state update is eventually committed, but takes longer than {@code ackTimeout} to do so, then the
     *                corresponding listener will be completed via {@link org.elasticsearch.cluster.ClusterStateAckListener#onAckTimeout()}
     *                when it is committed, and this may therefore happen some time after the timeout period elapses.
     *             </li>
     *         </ul>
     *         <p>
     *         A timeout of {@link TimeValue#MINUS_ONE} means that the master should wait indefinitely for acknowledgements.
     *         <p>
     *         A timeout of {@link TimeValue#ZERO} means that the master will complete this listener (via {@link #onAckTimeout()}) as soon
     *         as the state is committed, before any nodes have applied the new state.
     */
    TimeValue ackTimeout();

}
