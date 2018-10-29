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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TransportGetDiscoveredNodesAction extends TransportAction<GetDiscoveredNodesRequest, GetDiscoveredNodesResponse> {

    @Nullable // TODO make this not nullable
    private final Coordinator coordinator;
    private final TransportService transportService;

    @Inject
    public TransportGetDiscoveredNodesAction(Settings settings, ActionFilters actionFilters, TransportService transportService,
                                             Discovery discovery) {
        super(settings, GetDiscoveredNodesAction.NAME, actionFilters, transportService.getTaskManager());
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
            throw new IllegalStateException("cannot execute a Zen2 action if not using Zen2");
        }

        final DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        if (localNode.isMasterNode() == false) {
            throw new ElasticsearchException("this node is not master-eligible");
        }

        final AtomicBoolean responseSent = new AtomicBoolean();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final long timeoutMillis = request.timeout().millis();
        assert timeoutMillis >= 0 : timeoutMillis;

        final Consumer<Iterable<DiscoveryNode>> respondIfRequestSatisfied = new Consumer<Iterable<DiscoveryNode>>() {
            @Override
            public void accept(Iterable<DiscoveryNode> nodes) {
                final Set<DiscoveryNode> nodesSet = new LinkedHashSet<>();
                nodesSet.add(localNode);
                nodes.forEach(nodesSet::add);
                if (nodesSet.size() >= request.waitForNodes() && responseSent.compareAndSet(false, true)) {
                    listener.onResponse(new GetDiscoveredNodesResponse(nodesSet));
                    countDownLatch.countDown();
                }
            }

            @Override
            public String toString() {
                return "waiting for " + request;
            }
        };

        try (Releasable ignored = coordinator.withDiscoveryListener(respondIfRequestSatisfied)) {
            respondIfRequestSatisfied.accept(coordinator.getFoundPeers());
            if (timeoutMillis > 0 && countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                assert responseSent.get();
                return;
            }
        } catch (InterruptedException e) {
            logger.debug(new ParameterizedMessage("interrupted while waiting for {}", request), e);
            if (responseSent.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
            return;
        }

        if (responseSent.compareAndSet(false, true)) {
            listener.onFailure(new ElasticsearchTimeoutException("timed out while waiting for {}", request));
        }
    }
}
