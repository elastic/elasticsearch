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
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

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
