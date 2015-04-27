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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.node.service.NodeService;

/**
 * A pluggable module allowing to implement discovery of other nodes, publishing of the cluster
 * state to all nodes, electing a master of the cluster that raises cluster state change
 * events.
 */
public interface Discovery extends LifecycleComponent<Discovery> {

    DiscoveryNode localNode();

    void addListener(InitialStateDiscoveryListener listener);

    void removeListener(InitialStateDiscoveryListener listener);

    String nodeDescription();

    /**
     * Here as a hack to solve dep injection problem...
     */
    void setNodeService(@Nullable NodeService nodeService);

    /**
     * Another hack to solve dep injection problem..., note, this will be called before
     * any start is called.
     */
    void setAllocationService(AllocationService allocationService);

    /**
     * Publish all the changes to the cluster from the master (can be called just by the master). The publish
     * process should not publish this state to the master as well! (the master is sending it...).
     *
     * The {@link AckListener} allows to keep track of the ack received from nodes, and verify whether
     * they updated their own cluster state or not.
     */
    void publish(ClusterState clusterState, AckListener ackListener);

    public static interface AckListener {
        void onNodeAck(DiscoveryNode node, @Nullable Throwable t);
        void onTimeout();
    }
}
