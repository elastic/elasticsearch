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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class TransportBootstrapClusterAction extends TransportAction<BootstrapClusterRequest, AcknowledgedResponse> {

    @Nullable // TODO make this not nullable
    private final Coordinator coordinator;
    private final TransportService transportService;

    @Inject
    public TransportBootstrapClusterAction(Settings settings, ActionFilters actionFilters, TransportService transportService,
                                           Discovery discovery) {
        super(settings, BootstrapClusterAction.NAME, actionFilters, transportService.getTaskManager());
        this.transportService = transportService;
        if (discovery instanceof Coordinator) {
            coordinator = (Coordinator) discovery;
        } else {
            coordinator = null;
        }
    }

    @Override
    protected void doExecute(Task task, BootstrapClusterRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (coordinator == null) { // TODO remove when not nullable
            throw new IllegalStateException("cannot execute a Zen2 action if not using Zen2");
        }

        final DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        if (localNode.isMasterNode() == false) {
            throw new ElasticsearchException("this node is not master-eligible");
        }

        if (coordinator.isInitialConfigurationSet()) {
            logger.info("initial configuration already set, ignoring bootstrapping request");
            listener.onResponse(new AcknowledgedResponse(false));
        } else {
            final List<DiscoveryNode> selfAndDiscoveredPeers = new ArrayList<>();
            selfAndDiscoveredPeers.add(localNode);
            coordinator.getFoundPeers().forEach(selfAndDiscoveredPeers::add);

            final VotingConfiguration votingConfiguration = request.getBootstrapConfiguration().resolve(selfAndDiscoveredPeers);
            logger.info("setting initial configuration to {}", votingConfiguration);
            coordinator.setInitialConfiguration(votingConfiguration);
            listener.onResponse(new AcknowledgedResponse(true));
        }
    }
}
