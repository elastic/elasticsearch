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
 *
 */

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
 *
 */

package org.elasticsearch.discovery.single;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A discovery implementation where the only member of the cluster is the local node.
 */
public class SingleNodeDiscovery extends AbstractLifecycleComponent implements Discovery {

    private final ClusterService clusterService;
    private final DiscoverySettings discoverySettings;

    public SingleNodeDiscovery(final Settings settings, final ClusterService clusterService) {
        super(Objects.requireNonNull(settings));
        this.clusterService = Objects.requireNonNull(clusterService);
        final ClusterSettings clusterSettings =
                Objects.requireNonNull(clusterService.getClusterSettings());
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
    }

    @Override
    public DiscoveryNode localNode() {
        return clusterService.localNode();
    }

    @Override
    public String nodeDescription() {
        return clusterService.getClusterName().value() + "/" + clusterService.localNode().getId();
    }

    @Override
    public void setAllocationService(final AllocationService allocationService) {

    }

    @Override
    public void publish(final ClusterChangedEvent event, final AckListener listener) {

    }

    @Override
    public DiscoveryStats stats() {
        return null;
    }

    @Override
    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    @Override
    public void startInitialJoin() {
        final ClusterStateTaskExecutor<DiscoveryNode> executor =
                new ClusterStateTaskExecutor<DiscoveryNode>() {

                    @Override
                    public ClusterTasksResult<DiscoveryNode> execute(
                            final ClusterState current,
                            final List<DiscoveryNode> tasks) throws Exception {
                        assert tasks.size() == 1;
                        final DiscoveryNodes.Builder nodes =
                                DiscoveryNodes.builder(current.nodes());
                        // always set the local node as master, there will not be other nodes
                        nodes.masterNodeId(localNode().getId());
                        final ClusterState next =
                                ClusterState.builder(current).nodes(nodes).build();
                        final ClusterTasksResult.Builder<DiscoveryNode> result =
                                ClusterTasksResult.builder();
                        return result.successes(tasks).build(next);
                    }

                    @Override
                    public boolean runOnlyOnMaster() {
                        return false;
                    }

                };
        final ClusterStateTaskConfig config = ClusterStateTaskConfig.build(Priority.URGENT);
        clusterService.submitStateUpdateTasks(
                "single-node-start-initial-join",
                Collections.singletonMap(localNode(), (s, e) -> {}), config, executor);
    }

    @Override
    public int getMinimumMasterNodes() {
        return 1;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

}
