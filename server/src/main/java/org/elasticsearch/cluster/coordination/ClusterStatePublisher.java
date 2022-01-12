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
     * @param publishListener notified when the publication completes, whether successful or not. If successful then the new state has been
     *                        applied on this node and usually all other nodes (although some of them might have failed or timed out
     *                        instead). May fail with a {@link FailedToCommitClusterStateException} if this node didn't hear that enough
     *                        other master-eligible nodes accepted this state, which causes this node to stand down as master. On failure
     *                        the new state might or might not take effect in some future update: if it was accepted by the new master then
     *                        it will be applied eventually despite the exception.
     *
     * @param ackListener notified when individual nodes acknowledge that they've applied the cluster state (or failed to do so).
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
