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

package org.elasticsearch.client.transport;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.TransportAddress;

import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportClientNodesService extends AbstractComponent implements ClusterStateListener {

    private final TimeValue nodesSamplerInterval;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private volatile ImmutableList<TransportAddress> transportAddresses = ImmutableList.of();

    private final Object transportMutex = new Object();

    private volatile ImmutableList<DiscoveryNode> nodes = ImmutableList.of();

    private volatile DiscoveryNodes discoveredNodes;

    private final AtomicInteger tempNodeIdGenerator = new AtomicInteger();

    private final ScheduledNodesSampler nodesSampler = new ScheduledNodesSampler();

    private final ScheduledFuture nodesSamplerFuture;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger();

    @Inject public TransportClientNodesService(Settings settings, ClusterName clusterName,
                                               TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.clusterName = clusterName;
        this.transportService = transportService;
        this.threadPool = threadPool;

        this.nodesSamplerInterval = componentSettings.getAsTime("nodes_sampler_interval", timeValueSeconds(1));

        if (logger.isDebugEnabled()) {
            logger.debug("node_sampler_interval[" + nodesSamplerInterval + "]");
        }

        this.nodesSamplerFuture = threadPool.scheduleWithFixedDelay(nodesSampler, nodesSamplerInterval);

        // we want the transport service to throw connect exceptions, so we can retry
        transportService.throwConnectException(true);
    }

    public ImmutableList<TransportAddress> transportAddresses() {
        return this.transportAddresses;
    }

    public ImmutableList<DiscoveryNode> connectedNodes() {
        return this.nodes;
    }

    public TransportClientNodesService addTransportAddress(TransportAddress transportAddress) {
        synchronized (transportMutex) {
            ImmutableList.Builder<TransportAddress> builder = ImmutableList.builder();
            transportAddresses = builder.addAll(transportAddresses).add(transportAddress).build();
        }
        nodesSampler.run();
        return this;
    }

    public TransportClientNodesService removeTransportAddress(TransportAddress transportAddress) {
        synchronized (transportMutex) {
            ImmutableList.Builder<TransportAddress> builder = ImmutableList.builder();
            for (TransportAddress otherTransportAddress : transportAddresses) {
                if (!otherTransportAddress.equals(transportAddress)) {
                    builder.add(otherTransportAddress);
                }
            }
            transportAddresses = builder.build();
        }
        nodesSampler.run();
        return this;
    }

    public <T> T execute(NodeCallback<T> callback) throws ElasticSearchException {
        ImmutableList<DiscoveryNode> nodes = this.nodes;
        if (nodes.isEmpty()) {
            throw new NoNodeAvailableException();
        }
        int index = randomNodeGenerator.incrementAndGet();
        for (int i = 0; i < nodes.size(); i++) {
            DiscoveryNode node = nodes.get((index + i) % nodes.size());
            try {
                return callback.doWithNode(node);
            } catch (ConnectTransportException e) {
                // retry in this case
            }
        }
        throw new NoNodeAvailableException();
    }

    public void close() {
        nodesSamplerFuture.cancel(true);
    }

    @Override public void clusterChanged(ClusterChangedEvent event) {
        transportService.nodesAdded(event.nodesDelta().addedNodes());
        this.discoveredNodes = event.state().nodes();
        HashSet<DiscoveryNode> newNodes = new HashSet<DiscoveryNode>(nodes);
        newNodes.addAll(discoveredNodes.nodes().values());
        nodes = new ImmutableList.Builder<DiscoveryNode>().addAll(newNodes).build();
        transportService.nodesRemoved(event.nodesDelta().removedNodes());
    }

    private class ScheduledNodesSampler implements Runnable {

        @Override public synchronized void run() {
            ImmutableList<TransportAddress> transportAddresses = TransportClientNodesService.this.transportAddresses;
            final CountDownLatch latch = new CountDownLatch(transportAddresses.size());
            final CopyOnWriteArrayList<NodesInfoResponse> nodesInfoResponses = new CopyOnWriteArrayList<NodesInfoResponse>();
            final CopyOnWriteArrayList<DiscoveryNode> tempNodes = new CopyOnWriteArrayList<DiscoveryNode>();
            for (final TransportAddress transportAddress : transportAddresses) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        DiscoveryNode tempNode = new DiscoveryNode("#temp#-" + tempNodeIdGenerator.incrementAndGet(), transportAddress);
                        tempNodes.add(tempNode);
                        try {
                            transportService.nodesAdded(ImmutableList.of(tempNode));
                            transportService.sendRequest(tempNode, TransportActions.Admin.Cluster.Node.INFO, Requests.nodesInfo("_local"), new BaseTransportResponseHandler<NodesInfoResponse>() {

                                @Override public NodesInfoResponse newInstance() {
                                    return new NodesInfoResponse();
                                }

                                @Override public void handleResponse(NodesInfoResponse response) {
                                    nodesInfoResponses.add(response);
                                    latch.countDown();
                                }

                                @Override public void handleException(RemoteTransportException exp) {
                                    logger.debug("Failed to get node info from " + transportAddress + ", removed from nodes list", exp);
                                    latch.countDown();
                                }
                            });
                        } catch (Exception e) {
                            logger.debug("Failed to get node info from " + transportAddress + ", removed from nodes list", e);
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                return;
            }

            HashSet<DiscoveryNode> newNodes = new HashSet<DiscoveryNode>();
            for (NodesInfoResponse nodesInfoResponse : nodesInfoResponses) {
                if (nodesInfoResponse.nodes().length > 0) {
                    DiscoveryNode node = nodesInfoResponse.nodes()[0].node();
                    if (!clusterName.equals(nodesInfoResponse.clusterName())) {
                        logger.warn("Node {} not part of the cluster {}, ignoring...", node, clusterName);
                    } else {
                        newNodes.add(node);
                    }
                } else {
                    // should not really happen....
                    logger.debug("No info returned from node...");
                }
            }
            if (discoveredNodes != null) {
                newNodes.addAll(discoveredNodes.nodes().values());
            }
            nodes = new ImmutableList.Builder<DiscoveryNode>().addAll(newNodes).build();

            transportService.nodesRemoved(tempNodes);
        }
    }

    public static interface NodeCallback<T> {

        T doWithNode(DiscoveryNode node) throws ElasticSearchException;
    }
}
