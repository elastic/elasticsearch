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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Handles responses obtained when publishing a new cluster state from master to all non master nodes.
 * Allows to await a reply from all non master nodes, up to a timeout
 */
public class BlockingClusterStatePublishResponseHandler {

    private final CountDownLatch latch;
    private final Set<DiscoveryNode> pendingNodes;
    private final Set<DiscoveryNode> failedNodes;

    /**
     * Creates a new BlockingClusterStatePublishResponseHandler
     * @param publishingToNodes the set of nodes to which the cluster state will be published and should respond
     */
    public BlockingClusterStatePublishResponseHandler(Set<DiscoveryNode> publishingToNodes) {
        this.pendingNodes = ConcurrentCollections.newConcurrentSet();
        this.pendingNodes.addAll(publishingToNodes);
        this.latch = new CountDownLatch(pendingNodes.size());
        this.failedNodes = ConcurrentCollections.newConcurrentSet();
    }

    /**
     * Called for each response obtained from non master nodes
     *
     * @param node the node that replied to the publish event
     */
    public void onResponse(DiscoveryNode node) {
        boolean found = pendingNodes.remove(node);
        assert found : "node [" + node + "] already responded or failed";
        latch.countDown();
    }

    /**
     * Called for each failure obtained from non master nodes
     * @param node the node that replied to the publish event
     */
    public void onFailure(DiscoveryNode node) {
        boolean found = pendingNodes.remove(node);
        assert found : "node [" + node + "] already responded or failed";
        boolean added = failedNodes.add(node);
        assert added : "duplicate failures for " + node;
        latch.countDown();
    }

    /**
     * Allows to wait for all non master nodes to reply to the publish event up to a timeout
     * @param timeout the timeout
     * @return true if the timeout expired or not, false otherwise
     */
    public boolean awaitAllNodes(TimeValue timeout) throws InterruptedException {
        boolean success = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        assert !success || pendingNodes.isEmpty() : "response count reached 0 but still waiting for some nodes";
        return success;
    }

    /**
     * returns a list of nodes which didn't respond yet
     */
    public DiscoveryNode[] pendingNodes() {
        // we use a zero length array, because if we try to pre allocate we may need to remove trailing
        // nulls if some nodes responded in the meanwhile
        return pendingNodes.toArray(new DiscoveryNode[0]);
    }

    /**
     * returns a set of nodes for which publication has failed.
     */
    public Set<DiscoveryNode> getFailedNodes() {
        return Collections.unmodifiableSet(failedNodes);
    }
}
