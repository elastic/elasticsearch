/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

public interface ClusterStatePublisher {
    /**
     * Publish all the changes to the cluster from the master (can be called just by the master). The publish
     * process should apply this state to the master as well!
     *
     * The publishListener allows to wait for the publication to complete, which can be either successful completion, timing out or failing.
     * The method is guaranteed to pass back a {@link FailedToCommitClusterStateException} to the publishListener if the change is not
     * committed and should be rejected. Any other exception signals that something bad happened but the change is committed.
     *
     * The {@link AckListener} allows to keep track of the ack received from nodes, and verify whether
     * they updated their own cluster state or not.
     */
    void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener);

    interface AckListener {
        /**
         * Should be called when the cluster coordination layer has committed the cluster state (i.e. even if this publication fails,
         * it is guaranteed to appear in future publications).
         * @param commitTime the time it took to commit the cluster state
         */
        void onCommit(TimeValue commitTime);

        /**
         * Should be called whenever the cluster coordination layer receives confirmation from a node that it has successfully applied
         * the cluster state. In case of failures, an exception should be provided as parameter.
         * @param node the node
         * @param e the optional exception
         */
        void onNodeAck(DiscoveryNode node, @Nullable Exception e);
    }

}
