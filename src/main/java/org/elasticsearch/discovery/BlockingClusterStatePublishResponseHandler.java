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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Default implementation of {@link ClusterStatePublishResponseHandler}, allows to  await a reply
 * to a cluster state publish from all non master nodes, up to a timeout
 */
public class BlockingClusterStatePublishResponseHandler implements ClusterStatePublishResponseHandler {

    private final CountDownLatch latch;

    /**
     * Creates a new BlockingClusterStatePublishResponseHandler
     * @param nonMasterNodes number of nodes that are supposed to reply to a cluster state publish from master
     */
    public BlockingClusterStatePublishResponseHandler(int nonMasterNodes) {
        //Don't count the master, as it's the one that does the publish
        //the master won't call onResponse either
        this.latch = new CountDownLatch(nonMasterNodes);
    }

    @Override
    public void onResponse(DiscoveryNode node) {
        latch.countDown();
    }

    @Override
    public void onFailure(DiscoveryNode node, Throwable t) {
        latch.countDown();
    }

    @Override
    public boolean awaitAllNodes(TimeValue timeout) throws InterruptedException {
        return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
    }
}
