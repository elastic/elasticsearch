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

    private final AtomicInteger counter = new AtomicInteger();
    private final int expectedResponsesCount;
    private volatile AtomicReferenceArray<Object> responses;
    private volatile Exception causeOfDiscarding;

    public NodeResponseTracker(int size) {
        this.expectedResponsesCount = size;
        this.responses = new AtomicReferenceArray<>(size);
    }

    public NodeResponseTracker(Collection<Object> array) {
        this.expectedResponsesCount = array.size();
        this.responses = new AtomicReferenceArray<>(array.toArray());
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

    public boolean responsesDiscarded() {
        return responses == null;
    }

    /**
     * This method stores a new node response if the intermediate responses haven't been discarded yet. If the responses are not discarded
     * the method asserts that this is the first response encountered from this node to protect from miscounting the responses in case of a
     * double invocation. If the responses have been discarded we accept this risk for simplicity.
     * @param nodeIndex, the index that represents a single node of the cluster
     * @param response, a response can be either a NodeResponse or an error
     * @return true if all the nodes' responses have been received, else false
     */
    public boolean trackResponseAndCheckIfLast(int nodeIndex, Object response) {
        AtomicReferenceArray<Object> responses = this.responses;

        if (responsesDiscarded() == false) {
            boolean firstEncounter = responses.compareAndSet(nodeIndex, null, response);
            assert firstEncounter : "a response should be tracked only once";
        }
        return counter.incrementAndGet() == getExpectedResponseCount();
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

    public int getExpectedResponseCount() {
        return expectedResponsesCount;
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
