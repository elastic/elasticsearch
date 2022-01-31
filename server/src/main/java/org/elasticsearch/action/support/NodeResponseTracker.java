/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This class tracks the intermediate responses that can be used to construct the aggregated response. It also gives the possibility to
 * discard the intermediate results when asked, a use case for this call is when the corresponding task is cancelled.
 */
public class NodeResponseTracker {

    private static final DiscardedResponsesException DISCARDED_RESPONSES_EXCEPTION = new DiscardedResponsesException();

    private final int expectedResponses;
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicReferenceArray<Boolean> receivedResponseFromNode;
    private volatile AtomicReferenceArray<Object> responses;

    public NodeResponseTracker(int size) {
        this.expectedResponses = size;
        this.responses = new AtomicReferenceArray<>(size);
        this.receivedResponseFromNode = new AtomicReferenceArray<>(size);
    }

    public NodeResponseTracker(Collection<Object> array) {
        this.responses = new AtomicReferenceArray<>(array.toArray());
        this.expectedResponses = responses.length();
        this.receivedResponseFromNode = new AtomicReferenceArray<>(responses.length());
    }

    /**
     * This method discards the results collected to free up the resources
     */
    public void discardIntermediateResponses() {
        if (responses != null) {
            responses = null;
        }
    }

    public boolean allNodesResponded() {
        return expectedResponses == counter.get();
    }

    public boolean responsesDiscarded() {
        return responses == null;
    }

    /**
     * This method stores a new node response if it is first response encountered from this node and the intermediate responses haven't
     * been discarded yet. Checking that this is the first time we have received a response from this node is a defensive mechanism to
     * protect against the possibility of double invocation.
     * @param nodeIndex, the index that represents a single node of the cluster
     * @param response, a response can be either a NodeResponse or an error
     * @return true if it was successfully stored, else false
     */
    public boolean maybeAddResponse(int nodeIndex, Object response) {
        AtomicReferenceArray<Object> responses = this.responses;
        boolean firstEncounter = receivedResponseFromNode.compareAndSet(nodeIndex, null, true);
        if (firstEncounter) {
            counter.incrementAndGet();
        }
        if (responsesDiscarded() || firstEncounter == false) {
            return false;
        }

        responses.set(nodeIndex, response);
        return true;
    }

    public Object getResponse(int nodeIndex) throws DiscardedResponsesException {
        AtomicReferenceArray<Object> responses = this.responses;
        if (responsesDiscarded()) {
            throw DISCARDED_RESPONSES_EXCEPTION;
        }
        return responses.get(nodeIndex);
    }

    public int size() throws DiscardedResponsesException {
        if (responsesDiscarded()) {
            throw DISCARDED_RESPONSES_EXCEPTION;
        }
        return expectedResponses;
    }

    public static class DiscardedResponsesException extends Exception {}
}
