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
 * This class tracks the intermediate responses that will be used to create aggregated cluster response to a request. It also gives the
 * possibility to discard the intermediate results when asked, for example when the initial request is cancelled, in order to release the
 * resources.
 */
public class NodeResponseTracker {

    private final AtomicInteger receivedResponsesCounter = new AtomicInteger();
    private final AtomicReferenceArray<Boolean> receivedResponseFromNode;
    private volatile AtomicReferenceArray<Object> responses;
    private volatile Exception causeOfDiscarding;

    public NodeResponseTracker(int size) {
        this.responses = new AtomicReferenceArray<>(size);
        this.receivedResponseFromNode = new AtomicReferenceArray<>(size);
    }

    public NodeResponseTracker(Collection<Object> array) {
        this.responses = new AtomicReferenceArray<>(array.toArray());
        this.receivedResponseFromNode = new AtomicReferenceArray<>(responses.length());
    }

    /**
     * This method discards the results collected so far to free up the resources.
     * @param cause the discarding, this will be communicated if they try to access the discarded results
     */
    public void discardIntermediateResponses(Exception cause) {
        if (responses != null) {
            this.causeOfDiscarding = cause;
            responses = null;
        }
    }

    public boolean allNodesResponded() {
        return receivedResponseFromNode.length() == receivedResponsesCounter.get();
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
            receivedResponsesCounter.incrementAndGet();
        }
        if (responsesDiscarded() || firstEncounter == false) {
            return false;
        }

        responses.set(nodeIndex, response);
        return true;
    }

    /**
     * Returns the tracked response or null if the response hasn't been received yet for a specific index that represents a node of the
     * cluster.
     * @throws DiscardedResponsesException if the responses have been discarded
     */
    public Object getResponse(int nodeIndex) throws DiscardedResponsesException {
        AtomicReferenceArray<Object> responses = this.responses;
        if (responsesDiscarded()) {
            throw new DiscardedResponsesException(causeOfDiscarding);
        }
        return responses.get(nodeIndex);
    }

    /**
     * The count of the expected responses
     * @throws DiscardedResponsesException if the responses have been discarded
     */
    public int size() throws DiscardedResponsesException {
        if (responsesDiscarded()) {
            throw new DiscardedResponsesException(causeOfDiscarding);
        }
        return receivedResponseFromNode.length();
    }

    /**
     * This exception is thrown when the {@link NodeResponseTracker} is asked to give information about the responses after they have been
     * discarded.
     */
    public static class DiscardedResponsesException extends Exception {

        public DiscardedResponsesException(Exception cause) {
            super(cause);
        }
    }
}
