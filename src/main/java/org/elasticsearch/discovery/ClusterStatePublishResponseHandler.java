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

/**
 * Handles responses obtained when publishing a new cluster state from master to all non master nodes.
 * Allows to await a reply from all non master nodes, up to a timeout
 */
public interface ClusterStatePublishResponseHandler {

    /**
     * Called for each response obtained from non master nodes
     * @param node the node that replied to the publish event
     */
    void onResponse(DiscoveryNode node);

    /**
     * Called for each failure obtained from non master nodes
     * @param node the node that replied to the publish event
     */
    void onFailure(DiscoveryNode node, Throwable t);

    /**
     * Allows to wait for all non master nodes to reply to the publish event up to a timeout
     * @param timeout the timeout
     * @return true if the timeout expired or not, false otherwise
     * @throws InterruptedException
     */
    boolean awaitAllNodes(TimeValue timeout) throws InterruptedException;
}
