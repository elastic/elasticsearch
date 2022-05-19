/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

public interface ClusterStatePublisher {
    /**
     * Publishes an updated {@link ClusterState} to all the nodes in the cluster:
     *
     * <ul>
     *     <li>Send the updated state to all nodes. Typically we only send a {@link Diff} which each node combines with its local state to
     *     yield the updated state very cheaply.</li>
     *     <li>Wait for enough master-eligible nodes to indicate that they have <i>accepted</i> the new state (i.e. written it to durable
     *     storage).</li>
     *     <li>Tell all the nodes that the new state is now committed and should be applied via {@link
     *     ClusterStateApplier#applyClusterState}.</li>
     *     <li>Finally, apply the state on the master.</li>
     * </ul>
     *
     * @param publishListener Notified when the publication completes, whether successful or not. In particular, publication may fail with a
     *                        {@link FailedToCommitClusterStateException} if this node didn't receive responses to indicate that enough
     *                        other master-eligible nodes accepted this state. In that case this node stops being the elected master and the
     *                        master election process starts again.
     *                        <p>
     *                        If the publication completes successfully then every future state will be a descendant of the published state.
     *                        If the publication completes exceptionally then the new state may or may not be lost. More precisely, if the
     *                        published state was accepted by the node that wins the master election triggered by the publication failure
     *                        then the new master will publish the state which the old master failed to publish.
     *                        <p>
     *                        If the publication completes successfully then the new state has definitely been applied on this node, and it
     *                        has usually been applied on all other nodes too. However some nodes might have timed out or otherwise failed
     *                        to apply the state, so it is possible that the last-applied state on some nodes is somewhat stale.
     *
     * @param ackListener Notified when individual nodes acknowledge that they've applied the cluster state (or failed to do so).
     */
    void publish(ClusterStatePublicationEvent clusterStatePublicationEvent, ActionListener<Void> publishListener, AckListener ackListener);

    interface AckListener {
        /**
         * Should be called when the cluster coordination layer has committed the cluster state (i.e. even if this publication fails, it is
         * guaranteed to appear in future publications).
         * @param commitTime the time it took to commit the cluster state
         */
        void onCommit(TimeValue commitTime);

        /**
         * Should be called whenever the cluster coordination layer receives confirmation from a node that it has successfully applied the
         * cluster state. In case of failures, an exception should be provided as parameter.
         * @param node the node
         * @param e the optional exception
         */
        void onNodeAck(DiscoveryNode node, @Nullable Exception e);
    }

}
