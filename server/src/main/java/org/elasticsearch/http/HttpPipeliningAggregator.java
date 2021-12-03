/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class HttpPipeliningAggregator<Listener> {

    private final int maxEventsHeld;
    private final PriorityQueue<Tuple<HttpPipelinedResponse, Listener>> outboundHoldingQueue;
    /*
     * The current read and write sequence numbers. Read sequence numbers are attached to requests in the order they are read from the
     * channel, and then transferred to responses. A response is not written to the channel context until its sequence number matches the
     * current write sequence, implying that all preceding messages have been written.
     */
    private int readSequence;
    private int writeSequence;

    public HttpPipeliningAggregator(int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        this.outboundHoldingQueue = new PriorityQueue<>(1, Comparator.comparing(Tuple::v1));
    }

    public HttpPipelinedRequest read(final HttpRequest request) {
        return new HttpPipelinedRequest(readSequence++, request);
    }

    public List<Tuple<HttpPipelinedResponse, Listener>> write(final HttpPipelinedResponse response, Listener listener) {
        if (outboundHoldingQueue.size() < maxEventsHeld) {
            ArrayList<Tuple<HttpPipelinedResponse, Listener>> readyResponses = new ArrayList<>();
            outboundHoldingQueue.add(new Tuple<>(response, listener));
            while (outboundHoldingQueue.isEmpty() == false) {
                /*
                 * Since the response with the lowest sequence number is the top of the priority queue, we know if its sequence
                 * number does not match the current write sequence number then we have not processed all preceding responses yet.
                 */
                final Tuple<HttpPipelinedResponse, Listener> top = outboundHoldingQueue.peek();

                if (top.v1().getSequence() != writeSequence) {
                    break;
                }
                outboundHoldingQueue.poll();
                readyResponses.add(top);
                writeSequence++;
            }

            return readyResponses;
        } else {
            int eventCount = outboundHoldingQueue.size() + 1;
            throw new IllegalStateException("Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "].");
        }
    }

    public List<Tuple<HttpPipelinedResponse, Listener>> removeAllInflightResponses() {
        ArrayList<Tuple<HttpPipelinedResponse, Listener>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }
}
