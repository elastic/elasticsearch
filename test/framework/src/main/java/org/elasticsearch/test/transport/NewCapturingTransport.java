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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.lucene.util.LuceneTestCase.rarely;

public class NewCapturingTransport {

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
    private final MockTransportService mockTransportService;
    private final Transport transport;

    private final ConcurrentMap<Long, Tuple<DiscoveryNode, String>> requests = new ConcurrentHashMap<>();
    private final BlockingQueue<CapturingTransport.CapturedRequest> capturedRequests = ConcurrentCollections.newBlockingQueue();

    public NewCapturingTransport(MockTransportService mockTransportService) {
        this.mockTransportService = mockTransportService;
        this.transport = mockTransportService.transport();
    }

    /** returns all requests captured so far. Doesn't clear the captured request list. See {@link #clear()} */
    public CapturingTransport.CapturedRequest[] capturedRequests() {
        return capturedRequests.toArray(new CapturingTransport.CapturedRequest[0]);
    }

    /**
     * Returns all requests captured so far. This method does clear the
     * captured requests list. If you do not want the captured requests
     * list cleared, use {@link #capturedRequests()}.
     *
     * @return the captured requests
     */
    public CapturingTransport.CapturedRequest[] getCapturedRequestsAndClear() {
        List<CapturingTransport.CapturedRequest> requests = new ArrayList<>(capturedRequests.size());
        capturedRequests.drainTo(requests);
        return requests.toArray(new CapturingTransport.CapturedRequest[0]);
    }

    private Map<String, List<CapturingTransport.CapturedRequest>> groupRequestsByTargetNode(Collection<CapturingTransport.CapturedRequest> requests) {
        Map<String, List<CapturingTransport.CapturedRequest>> result = new HashMap<>();
        for (CapturingTransport.CapturedRequest request : requests) {
            result.computeIfAbsent(request.node.getId(), node -> new ArrayList<>()).add(request);
        }
        return result;
    }

    /**
     * returns all requests captured so far, grouped by target node.
     * Doesn't clear the captured request list. See {@link #clear()}
     */
    public Map<String, List<CapturingTransport.CapturedRequest>> capturedRequestsByTargetNode() {
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
    public Map<String, List<CapturingTransport.CapturedRequest>> getCapturedRequestsByTargetNodeAndClear() {
        List<CapturingTransport.CapturedRequest> requests = new ArrayList<>(capturedRequests.size());
        capturedRequests.drainTo(requests);
        return groupRequestsByTargetNode(requests);
    }

    /** clears captured requests */
    public void clear() {
        capturedRequests.clear();
    }

    /** simulate a response for the given requestId */
    public void handleResponse(final long requestId, final TransportResponse response) {
        // TODO: Null listener
        transport.getResponseHandlers().onResponseReceived(requestId, null).handleResponse(response);
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
        // TODO: Null listener
        transport.getResponseHandlers().onResponseReceived(requestId, null).handleException(e);
    }
}
