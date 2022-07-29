/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * A transport class that doesn't send anything but rather captures all requests for inspection from tests
 */
public class CapturingTransport extends MockTransport implements Transport {

    public record CapturedRequest(DiscoveryNode node, long requestId, String action, TransportRequest request) {}

    private BlockingQueue<CapturedRequest> capturedRequests = ConcurrentCollections.newBlockingQueue();

    /**
     * returns all requests captured so far. Doesn't clear the captured request list. See {@link #clear()}
     */
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

    /**
     * clears captured requests
     */
    public void clear() {
        capturedRequests.clear();
    }

    protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
        capturedRequests.add(new CapturingTransport.CapturedRequest(node, requestId, action, request));
    }
}
