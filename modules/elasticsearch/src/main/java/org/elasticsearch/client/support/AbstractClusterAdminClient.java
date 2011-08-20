/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.support;

import org.elasticsearch.client.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.restart.NodesRestartRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.shutdown.NodesShutdownRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.ping.broadcast.BroadcastPingRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.ping.replication.ReplicationPingRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.ping.single.SinglePingRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.client.internal.InternalClusterAdminClient;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractClusterAdminClient implements InternalClusterAdminClient {

    @Override public ClusterHealthRequestBuilder prepareHealth(String... indices) {
        return new ClusterHealthRequestBuilder(this).setIndices(indices);
    }

    @Override public ClusterStateRequestBuilder prepareState() {
        return new ClusterStateRequestBuilder(this);
    }

    @Override public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
        return new ClusterUpdateSettingsRequestBuilder(this);
    }

    @Override public NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds) {
        return new NodesInfoRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override public NodesStatsRequestBuilder prepareNodesStats(String... nodesIds) {
        return new NodesStatsRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override public NodesRestartRequestBuilder prepareNodesRestart(String... nodesIds) {
        return new NodesRestartRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override public NodesShutdownRequestBuilder prepareNodesShutdown(String... nodesIds) {
        return new NodesShutdownRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override public SinglePingRequestBuilder preparePingSingle() {
        return new SinglePingRequestBuilder(this);
    }

    @Override public SinglePingRequestBuilder preparePingSingle(String index, String type, String id) {
        return preparePingSingle().setIndex(index).setType(type).setId(id);
    }

    @Override public BroadcastPingRequestBuilder preparePingBroadcast(String... indices) {
        return new BroadcastPingRequestBuilder(this).setIndices(indices);
    }

    @Override public ReplicationPingRequestBuilder preparePingReplication(String... indices) {
        return new ReplicationPingRequestBuilder(this).setIndices(indices);
    }
}
