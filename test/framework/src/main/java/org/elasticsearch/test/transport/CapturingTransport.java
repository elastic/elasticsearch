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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportServiceAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** A transport class that doesn't send anything but rather captures all requests for inspection from tests */
public class CapturingTransport implements Transport {

    private TransportServiceAdapter adapter;

    static public class CapturedRequest {
        final public DiscoveryNode node;
        final public long requestId;
        final public String action;
        final public TransportRequest request;

        public CapturedRequest(DiscoveryNode node, long requestId, String action, TransportRequest request) {
            this.node = node;
            this.requestId = requestId;
            this.action = action;
            this.request = request;
        }
    }

    private ConcurrentMap<Long, Tuple<DiscoveryNode, String>> requests = new ConcurrentHashMap<>();
    private BlockingQueue<CapturedRequest> capturedRequests = ConcurrentCollections.newBlockingQueue();

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
        adapter.onResponseReceived(requestId).handleResponse(response);
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
        this.handleError(requestId, new RemoteTransportException("remote failure", t));
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
        adapter.onResponseReceived(requestId).handleException(e);
    }

    @Override
    public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options)
        throws IOException, TransportException {
        requests.put(requestId, Tuple.tuple(node, action));
        capturedRequests.add(new CapturedRequest(node, requestId, action, request));
    }


    @Override
    public void transportServiceAdapter(TransportServiceAdapter adapter) {
        this.adapter = adapter;
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
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws Exception {
        return new TransportAddress[0];
    }

    @Override
    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return false;
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return true;
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

    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {

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
    public List<String> getLocalAddresses() {
        return Collections.emptyList();
    }

}
