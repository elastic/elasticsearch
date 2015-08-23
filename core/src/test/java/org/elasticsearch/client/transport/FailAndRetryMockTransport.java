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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

abstract class FailAndRetryMockTransport<Response extends TransportResponse> implements Transport {

    private final Random random;

    private boolean connectMode = true;

    private TransportServiceAdapter transportServiceAdapter;

    private final AtomicInteger connectTransportExceptions = new AtomicInteger();
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicInteger successes = new AtomicInteger();
    private final Set<DiscoveryNode> triedNodes = new CopyOnWriteArraySet<>();

    FailAndRetryMockTransport(Random random) {
        this.random = new Random(random.nextLong());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {

        //we make sure that nodes get added to the connected ones when calling addTransportAddress, by returning proper nodes info
        if (connectMode) {
            TransportResponseHandler transportResponseHandler = transportServiceAdapter.onResponseReceived(requestId);
            transportResponseHandler.handleResponse(new LivenessResponse(ClusterName.DEFAULT, node));
            return;
        }

        //once nodes are connected we'll just return errors for each sendRequest call
        triedNodes.add(node);

        if (RandomInts.randomInt(random, 100) > 10) {
            connectTransportExceptions.incrementAndGet();
            throw new ConnectTransportException(node, "node not available");
        } else {
            if (random.nextBoolean()) {
                failures.incrementAndGet();
                //throw whatever exception that is not a subclass of ConnectTransportException
                throw new IllegalStateException();
            } else {
                TransportResponseHandler transportResponseHandler = transportServiceAdapter.onResponseReceived(requestId);
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
    public void transportServiceAdapter(TransportServiceAdapter transportServiceAdapter) {
        this.transportServiceAdapter = transportServiceAdapter;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addressSupported(Class<? extends TransportAddress> address) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return false;
    }

    @Override
    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {

    }

    @Override
    public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {

    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {

    }

    @Override
    public long serverOpen() {
        return 0;
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
    public Transport start() {
        return null;
    }

    @Override
    public Transport stop() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return Collections.EMPTY_MAP;
    }
}
