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
package org.elasticsearch.test.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public class NoopClusterService implements ClusterService {

    final ClusterState state;

    public NoopClusterService() {
        this(ClusterState.builder(new ClusterName("noop")).build());
    }

    public NoopClusterService(ClusterState state) {
        if (state.getNodes().size() == 0) {
            state = ClusterState.builder(state).nodes(
                    DiscoveryNodes.builder()
                            .put(new DiscoveryNode("noop_id", DummyTransportAddress.INSTANCE, Version.CURRENT))
                            .localNodeId("noop_id")).build();
        }

        assert state.getNodes().localNode() != null;
        this.state = state;

    }

    @Override
    public DiscoveryNode localNode() {
        return state.getNodes().localNode();
    }

    @Override
    public ClusterState state() {
        return state;
    }

    @Override
    public void addInitialStateBlock(ClusterBlock block) throws IllegalStateException {

    }

    @Override
    public void removeInitialStateBlock(ClusterBlock block) throws IllegalStateException {

    }

    @Override
    public OperationRouting operationRouting() {
        return null;
    }

    @Override
    public void addFirst(ClusterStateListener listener) {

    }

    @Override
    public void addLast(ClusterStateListener listener) {

    }

    @Override
    public void add(ClusterStateListener listener) {

    }

    @Override
    public void remove(ClusterStateListener listener) {

    }

    @Override
    public void add(LocalNodeMasterListener listener) {

    }

    @Override
    public void remove(LocalNodeMasterListener listener) {

    }

    @Override
    public void add(TimeValue timeout, TimeoutClusterStateListener listener) {

    }

    @Override
    public void submitStateUpdateTask(String source, Priority priority, ClusterStateUpdateTask updateTask) {

    }

    @Override
    public void submitStateUpdateTask(String source, ClusterStateUpdateTask updateTask) {

    }

    @Override
    public List<PendingClusterTask> pendingTasks() {
        return null;
    }

    @Override
    public int numberOfPendingTasks() {
        return 0;
    }

    @Override
    public TimeValue getMaxTaskWaitTime() {
        return TimeValue.timeValueMillis(0);
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public ClusterService start() {
        return null;
    }

    @Override
    public ClusterService stop() {
        return null;
    }

    @Override
    public void close() {

    }
}
