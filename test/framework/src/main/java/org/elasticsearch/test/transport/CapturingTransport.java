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

package org.elasticsearch.test.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.lucene.util.LuceneTestCase.rarely;

/** A transport class that doesn't send anything but rather captures all requests for inspection from tests */
public class CapturingTransport implements Transport {

    private TransportService transportService;

    public static class CapturedRequest {
        public final DiscoveryNode node;
        public final long requestId;
        public final String action;
        public final TransportRequest request;

        public CapturedRequest(DiscoveryNode node, long requestId, String action, TransportRequest request) {
            this.node = node;
            this.requestId = requestId;
            this.action = action;
            this.request = request;
        }
    }

    private ConcurrentMap<Long, Tuple<DiscoveryNode, String>> requests = new ConcurrentHashMap<>();
    private BlockingQueue<CapturedRequest> capturedRequests = ConcurrentCollections.newBlockingQueue();
    private final AtomicLong requestId = new AtomicLong();


    /** returns all requests captured so far. Doesn't clear the captured request list. See {@link #clear()} */
    public CapturedRequest[] capturedRequests() {
        return capturedRequests.toArray(new CapturedRequest[0]);
    }

    /**
     * Returns all requests captured so far. This method does clear the
     * captured requests list. If you do not want the captured requests
     * list cleared, use {@link #capturedRequests()}.
     *
     * @return the captured requests
     */
    public CapturedRequest[] getCapturedRequestsAndClear() {
        CapturedRequest[] capturedRequests = capturedRequests();
        clear();
        return capturedRequests;
    }

    /**
     * returns all requests captured so far, grouped by target node.
     * Doesn't clear the captured request list. See {@link #clear()}
     */
    public Map<String, List<CapturedRequest>> capturedRequestsByTargetNode() {
        Map<String, List<CapturedRequest>> map = new HashMap<>();
        for (CapturedRequest request : capturedRequests) {
            List<CapturedRequest> nodeList = map.get(request.node.getId());
            if (nodeList == null) {
                nodeList = new ArrayList<>();
                map.put(request.node.getId(), nodeList);
            }
            nodeList.add(request);
        }
        return map;
    }

    /**
     * Returns all requests captured so far, grouped by target node.
     * This method does clear the captured request list. If you do not
     * want the captured requests list cleared, use
     * {@link #capturedRequestsByTargetNode()}.
     *
     * @return the captured requests grouped by target node
     */
    public Map<String, List<CapturedRequest>> getCapturedRequestsByTargetNodeAndClear() {
        Map<String, List<CapturedRequest>> map = capturedRequestsByTargetNode();
        clear();
        return map;
    }

    /** clears captured requests */
    public void clear() {
        capturedRequests.clear();
    }

    /** simulate a response for the given requestId */
    public void handleResponse(final long requestId, final TransportResponse response) {
        transportService.onResponseReceived(requestId).handleResponse(response);
    }

    /**
     * simulate a local error for the given requestId, will be wrapped
     * by a {@link SendRequestTransportException}
     *
     * @param requestId the id corresponding to the captured send
     *                  request
     * @param t the failure to wrap
     */
    public void handleLocalError(final long requestId, final Throwable t) {
        Tuple<DiscoveryNode, String> request = requests.get(requestId);
        assert request != null;
        this.handleError(requestId, new SendRequestTransportException(request.v1(), request.v2(), t));
    }

    /**
     * simulate a remote error for the given requestId, will be wrapped
     * by a {@link RemoteTransportException}
     *
     * @param requestId the id corresponding to the captured send
     *                  request
     * @param t the failure to wrap
     */
    public void handleRemoteError(final long requestId, final Throwable t) {
        final RemoteTransportException remoteException;
        if (rarely(Randomness.get())) {
            remoteException = new RemoteTransportException("remote failure, coming from local node", t);
        } else {
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeException(t);
                remoteException = new RemoteTransportException("remote failure", output.bytes().streamInput().readException());
            } catch (IOException ioException) {
                throw new ElasticsearchException("failed to serialize/deserialize supplied exception " + t, ioException);
            }
        }
        this.handleError(requestId, remoteException);
    }

    /**
     * simulate an error for the given requestId, unlike
     * {@link #handleLocalError(long, Throwable)} and
     * {@link #handleRemoteError(long, Throwable)}, the provided
     * exception will not be wrapped but will be delivered to the
     * transport layer as is
     *
     * @param requestId the id corresponding to the captured send
     *                  request
     * @param e the failure
     */
    public void handleError(final long requestId, final TransportException e) {
        transportService.onResponseReceived(requestId).handleException(e);
    }

    @Override
    public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
        return new Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws IOException, TransportException {
                requests.put(requestId, Tuple.tuple(node, action));
                capturedRequests.add(new CapturedRequest(node, requestId, action, request));
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public TransportStats getStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return new TransportAddress[0];
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return true;
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

    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void close() {}

    @Override
    public List<String> getLocalAddresses() {
        return Collections.emptyList();
    }

    @Override
    public long newRequestId() {
        return requestId.incrementAndGet();
    }

    public Connection getConnection(DiscoveryNode node) {
        try {
            return openConnection(node, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
