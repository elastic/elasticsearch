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

package org.elasticsearch.client.transport;

import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

abstract class FailAndRetryMockTransport<Response extends TransportResponse> implements Transport {

    private final Random random;
    private final ClusterName clusterName;

    private boolean connectMode = true;

    private TransportService transportService;

    private final AtomicInteger connectTransportExceptions = new AtomicInteger();
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicInteger successes = new AtomicInteger();
    private final Set<DiscoveryNode> triedNodes = new CopyOnWriteArraySet<>();
    private final AtomicLong requestId = new AtomicLong();

    FailAndRetryMockTransport(Random random, ClusterName clusterName) {
        this.random = new Random(random.nextLong());
        this.clusterName = clusterName;
    }

    protected abstract ClusterState getMockClusterState(DiscoveryNode node);

    @Override
    public Connection getConnection(DiscoveryNode node) {
        return new Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws IOException, TransportException {

                //we make sure that nodes get added to the connected ones when calling addTransportAddress, by returning proper nodes info
                if (connectMode) {
                    if (TransportLivenessAction.NAME.equals(action)) {
                        TransportResponseHandler transportResponseHandler = transportService.onResponseReceived(requestId);
                        transportResponseHandler.handleResponse(new LivenessResponse(ClusterName.CLUSTER_NAME_SETTING.
                            getDefault(Settings.EMPTY),
                            node));
                    } else if (ClusterStateAction.NAME.equals(action)) {
                        TransportResponseHandler transportResponseHandler = transportService.onResponseReceived(requestId);
                        ClusterState clusterState = getMockClusterState(node);
                        transportResponseHandler.handleResponse(new ClusterStateResponse(clusterName, clusterState, 0L));
                    } else {
                        throw new UnsupportedOperationException("Mock transport does not understand action " + action);
                    }
                    return;
                }

                //once nodes are connected we'll just return errors for each sendRequest call
                triedNodes.add(node);

                if (random.nextInt(100) > 10) {
                    connectTransportExceptions.incrementAndGet();
                    throw new ConnectTransportException(node, "node not available");
                } else {
                    if (random.nextBoolean()) {
                        failures.incrementAndGet();
                        //throw whatever exception that is not a subclass of ConnectTransportException
                        throw new IllegalStateException();
                    } else {
                        TransportResponseHandler transportResponseHandler = transportService.onResponseReceived(requestId);
                        if (random.nextBoolean()) {
                            successes.incrementAndGet();
                            transportResponseHandler.handleResponse(newResponse());
                        } else {
                            failures.incrementAndGet();
                            transportResponseHandler.handleException(new TransportException("transport exception"));
                        }
                    }
                }
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
        return getConnection(node);
    }

    protected abstract Response newResponse();

    public void endConnectMode() {
        this.connectMode = false;
    }

    public int connectTransportExceptions() {
        return connectTransportExceptions.get();
    }

    public int failures() {
        return failures.get();
    }

    public int successes() {
        return successes.get();
    }

    public Set<DiscoveryNode> triedNodes() {
        return triedNodes;
    }

    @Override
    public void setTransportService(TransportService transportServiceAdapter) {
        this.transportService = transportServiceAdapter;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return false;
    }

    @Override
    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
        throws ConnectTransportException {

    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {

    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void close() {}

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return Collections.emptyMap();
    }

    @Override
    public long newRequestId() {
        return requestId.incrementAndGet();
    }

    @Override
    public TransportStats getStats() {
        throw new UnsupportedOperationException();
    }
}
