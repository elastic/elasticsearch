/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 * An implementation of {@link ClusterStateUpdateTask} that allows to be notified when
 * all the nodes have acknowledged a cluster state update request
 */
public abstract class AckedClusterStateUpdateTask<Response extends ClusterStateUpdateResponse> implements TimeoutClusterStateUpdateTask {

    private final ClusterStateUpdateRequest<?> clusterStateUpdateRequest;
    private final ClusterStateUpdateListener<Response> listener;

    protected AckedClusterStateUpdateTask(ClusterStateUpdateRequest<?> clusterStateUpdateRequest, ClusterStateUpdateListener<Response> listener) {
        this.clusterStateUpdateRequest = clusterStateUpdateRequest;
        this.listener = listener;
    }

    /**
     * Called to determine which nodes the acknowledgement is expected from
     * By default the ack is expected from all nodes
     * @param discoveryNode a node
     * @return true if the node is expected to send ack back, false otherwise
     */
    public boolean mustAck(DiscoveryNode discoveryNode) {
        return true;
    }

    /**
     * Called once all the nodes have acknowledged the cluster state update request. Must be
     * very lightweight execution, since it gets executed on the cluster service thread.
     * @param t optional error that might have been thrown
     */
    public void onAllNodesAcked(@Nullable Throwable t) {
        beforeReturn();
        listener.onResponse(newResponse(true));
    }

    /**
     * Called once the acknowledgement timeout defined by
     * {@link AckedClusterStateUpdateTask#ackTimeout()} has expired
     */
    public void onAckTimeout() {
        beforeReturn();
        listener.onResponse(newResponse(false));
    }

    /**
     * We do nothing by default before returning
     * Handy to subclass in case we need to release a lock for instance
     */
    protected void beforeReturn() {

    }

    /**
     * Creates a new cluster state update response based on the acknowledged flag
     */
    protected abstract Response newResponse(boolean acknowledged);

    /**
     * Acknowledgement timeout, maximum time interval to wait for acknowledgements
     */
    public TimeValue ackTimeout() {
        return clusterStateUpdateRequest.ackTimeout();
    }

    @Override
    public void onFailure(String source, Throwable t) {
        beforeReturn();
        listener.onFailure(t);
    }

    @Override
    public TimeValue timeout() {
        return clusterStateUpdateRequest.masterNodeTimeout();
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
    }
}
