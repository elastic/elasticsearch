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
 * This class collects and tracks the intermediate responses that can be used to construct the aggregated response. It also gives the
 * possibility to discard the intermediate results when asked, a use case for this call is when a task is cancelled.
 */
public class IntermediateNodeResponses {

    private static final AlreadyDiscardedException ALREADY_DISCARDED_EXCEPTION = new AlreadyDiscardedException();

    private final int expectedResponses;
    private final AtomicInteger counter = new AtomicInteger();
    private volatile AtomicReferenceArray<Object> responses;
    private volatile boolean discarded = false;

    public IntermediateNodeResponses(int size) {
        this.expectedResponses = size;
        this.responses = new AtomicReferenceArray<>(size);
    }

    public IntermediateNodeResponses(Collection<Object> array) {
        this.responses = new AtomicReferenceArray<>(array.toArray());
        this.expectedResponses = responses.length();
    }

    /**
     * This method marks the responses as discarded and discards the results collected to free up the resources
     */
    public void discard() {
        if (discarded == false) {
            discarded = true;
            responses = null;
        }
    }

    public boolean isComplete() {
        return discarded == false && expectedResponses == counter.get();
    }

    public boolean isDiscarded() {
        return discarded;
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
        if (discarded) {
            return false;
        }
        if (responses.compareAndSet(nodeIndex, null, response)) {
            counter.incrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    public Object getResponse(int nodeIndex) throws AlreadyDiscardedException {
        AtomicReferenceArray<Object> responses = this.responses;
        if (discarded) {
            throw ALREADY_DISCARDED_EXCEPTION;
        }
        return responses.get(nodeIndex);
    }

    public int size() throws AlreadyDiscardedException {
        if (discarded) {
            throw ALREADY_DISCARDED_EXCEPTION;
        }
        return expectedResponses;
    }

    public static class AlreadyDiscardedException extends Exception {}
}
