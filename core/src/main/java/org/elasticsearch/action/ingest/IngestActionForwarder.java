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

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility for forwarding ingest requests to ingest nodes in a round-robin fashion.
 *
 * TODO: move this into IngestService and make index/bulk actions call that
 */
public final class IngestActionForwarder implements ClusterStateApplier {

    private final TransportService transportService;
    private final AtomicInteger ingestNodeGenerator = new AtomicInteger(Randomness.get().nextInt());
    private DiscoveryNode[] ingestNodes;

    public IngestActionForwarder(TransportService transportService) {
        this.transportService = transportService;
        ingestNodes = new DiscoveryNode[0];
    }

    public void forwardIngestRequest(Action<?, ?, ?> action, ActionRequest request, ActionListener<?> listener) {
        transportService.sendRequest(randomIngestNode(), action.name(), request,
            new ActionListenerResponseHandler(listener, action::newResponse));
    }

    private DiscoveryNode randomIngestNode() {
        final DiscoveryNode[] nodes = ingestNodes;
        if (nodes.length == 0) {
            throw new IllegalStateException("There are no ingest nodes in this cluster, unable to forward request to an ingest node.");
        }

        return nodes[Math.floorMod(ingestNodeGenerator.incrementAndGet(), nodes.length)];
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        ingestNodes = event.state().getNodes().getIngestNodes().values().toArray(DiscoveryNode.class);
    }
}
