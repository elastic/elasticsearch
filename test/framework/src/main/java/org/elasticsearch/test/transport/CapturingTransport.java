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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.lucene.util.LuceneTestCase.rarely;

/** A transport class that doesn't send anything but rather captures all requests for inspection from tests */
public class CapturingTransport implements Transport {

    private volatile Map<String, RequestHandlerRegistry> requestHandlers = Collections.emptyMap();
    final Object requestHandlerMutex = new Object();
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    private TransportMessageListener listener;

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

    public TransportService createCapturingTransportService(Settings settings, ThreadPool threadPool, TransportInterceptor interceptor,
                                                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                            @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders) {
        StubbableConnectionManager connectionManager = new StubbableConnectionManager(new ConnectionManager(settings, this, threadPool),
            settings, this, threadPool);
        connectionManager.setDefaultNodeConnectedBehavior((cm, discoveryNode) -> true);
        connectionManager.setDefaultConnectBehavior((cm, discoveryNode) -> new Connection() {
            @Override
            public DiscoveryNode getNode() {
                return discoveryNode;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {
                requests.put(requestId, Tuple.tuple(discoveryNode, action));
                capturedRequests.add(new CapturedRequest(discoveryNode, requestId, action, request));
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {

            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {

            }
        });
        return new TransportService(settings, this, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders,
            connectionManager);

    }

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
        List<CapturedRequest> requests = new ArrayList<>(capturedRequests.size());
        capturedRequests.drainTo(requests);
        return requests.toArray(new CapturedRequest[0]);
    }

    private Map<String, List<CapturedRequest>> groupRequestsByTargetNode(Collection<CapturedRequest> requests) {
        Map<String, List<CapturedRequest>> result = new HashMap<>();
        for (CapturedRequest request : requests) {
            result.computeIfAbsent(request.node.getId(), node -> new ArrayList<>()).add(request);
        }
        return result;
    }

    /**
     * returns all requests captured so far, grouped by target node.
     * Doesn't clear the captured request list. See {@link #clear()}
     */
    public Map<String, List<CapturedRequest>> capturedRequestsByTargetNode() {
        return groupRequestsByTargetNode(capturedRequests);
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
        List<CapturedRequest> requests = new ArrayList<>(capturedRequests.size());
        capturedRequests.drainTo(requests);
        return groupRequestsByTargetNode(requests);
    }

    /** clears captured requests */
    public void clear() {
        capturedRequests.clear();
    }

    /** simulate a response for the given requestId */
    public void handleResponse(final long requestId, final TransportResponse response) {
        responseHandlers.onResponseReceived(requestId, listener).handleResponse(response);
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
        responseHandlers.onResponseReceived(requestId, listener).handleException(e);
    }

    @Override
    public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) {
        return new Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {
                onSendRequest(requestId, action, request, node);
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {

            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {
            }
        };
    }

    protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
        requests.put(requestId, Tuple.tuple(node, action));
        capturedRequests.add(new CapturedRequest(node, requestId, action, request));
    }

    @Override
    public TransportStats getStats() {
        throw new UnsupportedOperationException();
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
    public <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }
    }
    @Override
    public ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public RequestHandlerRegistry getRequestHandler(String action) {
        return requestHandlers.get(action);
    }

    @Override
    public void addMessageListener(TransportMessageListener listener) {
        if (this.listener != null) {
            throw new IllegalStateException("listener already set");
        }
        this.listener = listener;
    }

    @Override
    public boolean removeMessageListener(TransportMessageListener listener) {
        if (listener == this.listener) {
            this.listener = null;
            return true;
        }
        return false;
    }

}
