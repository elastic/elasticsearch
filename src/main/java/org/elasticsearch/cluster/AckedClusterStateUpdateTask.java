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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 * An extension interface to {@link ClusterStateUpdateTask} that allows to be notified when
 * all the nodes have acknowledged a cluster state update request
 */
public abstract class AckedClusterStateUpdateTask<T extends AcknowledgedRequest, R extends AcknowledgedResponse> implements TimeoutClusterStateUpdateTask {

    protected final T request;
    private final boolean mustAck;
    private final ActionListener<R> responseListener;

    protected AckedClusterStateUpdateTask(T request, boolean mustAck, ActionListener<R> responseListener) {
        this.request = request;
        this.mustAck = mustAck;
        this.responseListener = responseListener;

    }


    /**
     * Called to determine which nodes the acknowledgement is expected from
     * @param discoveryNode a node
     * @return true if the node is expected to send ack back, false otherwise
     */
    public boolean mustAck(DiscoveryNode discoveryNode) {
        return mustAck;
    }

    @Override
    public void onFailure(String source, Throwable t) {
        responseListener.onFailure(t);
    }

    /**
     * Called once all the nodes have acknowledged the cluster state update request. Must be
     * very lightweight execution, since it gets executed on the cluster service thread.
     * @param t optional error that might have been thrown
     */
    public void onAllNodesAcked(@Nullable Throwable t) {
        responseListener.onResponse(newResponse(true));
    }

    /**
     * Called once the acknowledgement timeout defined by
     * {@link AckedClusterStateUpdateTask#ackTimeout()} has expired
     */
    public void onAckTimeout() {
        responseListener.onResponse(newResponse(false));
    }

    /**
     * Acknowledgement timeout, maximum time interval to wait for acknowledgements
     */
    public TimeValue ackTimeout() {
        return request.timeout();
    }

    @Override
    public TimeValue timeout() {
        return request.masterNodeTimeout();
    }

    protected abstract R newResponse(boolean acked);

}
