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

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;

public class TransportGetDiscoveredNodesAction extends HandledTransportAction<GetDiscoveredNodesRequest, GetDiscoveredNodesResponse> {

    @Nullable // TODO make this not nullable
    private final Coordinator coordinator;
    private final TransportService transportService;
    private final String discoveryType;

    @Inject
    public TransportGetDiscoveredNodesAction(Settings settings, ActionFilters actionFilters, TransportService transportService,
                                             Discovery discovery) {
        super(GetDiscoveredNodesAction.NAME, transportService, actionFilters,
            (Reader<GetDiscoveredNodesRequest>) GetDiscoveredNodesRequest::new);

        this.discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        this.transportService = transportService;
        if (discovery instanceof Coordinator) {
            coordinator = (Coordinator) discovery;
        } else {
            coordinator = null;
        }
    }

    @Override
    protected void doExecute(Task task, GetDiscoveredNodesRequest request, ActionListener<GetDiscoveredNodesResponse> listener) {
        if (coordinator == null) { // TODO remove when not nullable
            throw new IllegalArgumentException("discovered nodes are not exposed by discovery type [" + discoveryType + "]");
        }

        final DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        if (localNode.isMasterNode() == false) {
            throw new IllegalArgumentException(
                "this node is not master-eligible, but discovered nodes are only exposed by master-eligible nodes");
        }
        final ExecutorService directExecutor = EsExecutors.newDirectExecutorService();
        final AtomicBoolean listenerNotified = new AtomicBoolean();
        final ListenableFuture<GetDiscoveredNodesResponse> listenableFuture = new ListenableFuture<>();
        final ThreadPool threadPool = transportService.getThreadPool();
        listenableFuture.addListener(listener, directExecutor, threadPool.getThreadContext());
        // TODO make it so that listenableFuture copes with multiple completions, and then remove listenerNotified

        final Consumer<Iterable<DiscoveryNode>> respondIfRequestSatisfied = new Consumer<Iterable<DiscoveryNode>>() {
            @Override
            public void accept(Iterable<DiscoveryNode> nodes) {
                final Set<DiscoveryNode> nodesSet = new LinkedHashSet<>();
                nodesSet.add(localNode);
                nodes.forEach(nodesSet::add);
                logger.trace("discovered {}", nodesSet);
                try {
                    if (checkWaitRequirements(request, nodesSet) && listenerNotified.compareAndSet(false, true)) {
                        listenableFuture.onResponse(new GetDiscoveredNodesResponse(nodesSet));
                    }
                } catch (Exception e) {
                    listenableFuture.onFailure(e);
                }
            }

            @Override
            public String toString() {
                return "waiting for " + request;
            }
        };

        final Releasable releasable = coordinator.withDiscoveryListener(respondIfRequestSatisfied);
        listenableFuture.addListener(ActionListener.wrap(releasable::close), directExecutor, threadPool.getThreadContext());
        respondIfRequestSatisfied.accept(coordinator.getFoundPeers());

        if (request.getTimeout() != null) {
            threadPool.schedule(request.getTimeout(), Names.SAME, new Runnable() {
                @Override
                public void run() {
                    if (listenerNotified.compareAndSet(false, true)) {
                        listenableFuture.onFailure(new ElasticsearchTimeoutException("timed out while waiting for " + request));
                    }
                }

                @Override
                public String toString() {
                    return "timeout handler for " + request;
                }
            });
        }
    }

    private static boolean matchesRequirement(DiscoveryNode discoveryNode, String requirement) {
        return discoveryNode.getName().equals(requirement)
            || discoveryNode.getAddress().toString().equals(requirement)
            || discoveryNode.getAddress().getAddress().equals(requirement);
    }

    private static boolean checkWaitRequirements(GetDiscoveredNodesRequest request, Set<DiscoveryNode> nodes) {
        if (nodes.size() < request.getWaitForNodes()) {
            return false;
        }

        List<String> requirements = request.getRequiredNodes();
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        for (final String requirement : requirements) {
            final Set<DiscoveryNode> matchingNodes
                = nodes.stream().filter(n -> matchesRequirement(n, requirement)).collect(Collectors.toSet());

            if (matchingNodes.isEmpty()) {
                return false;
            }
            if (matchingNodes.size() > 1) {
                throw new IllegalArgumentException("[" + requirement + "] matches " + matchingNodes);
            }

            for (final DiscoveryNode matchingNode : matchingNodes) {
                if (selectedNodes.add(matchingNode) == false) {
                    throw new IllegalArgumentException("[" + matchingNode + "] matches " +
                        requirements.stream().filter(r -> matchesRequirement(matchingNode, requirement)).collect(Collectors.toList()));
                }
            }
        }

        return true;
    }
}
