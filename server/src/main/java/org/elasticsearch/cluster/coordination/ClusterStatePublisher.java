/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

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
