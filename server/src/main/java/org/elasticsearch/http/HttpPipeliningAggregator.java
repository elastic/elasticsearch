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
package org.elasticsearch.http;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class HttpPipeliningAggregator<Response, Listener> {

    private final int maxEventsHeld;
    private final PriorityQueue<HttpPipelinedResponse<Response, Listener>> outboundHoldingQueue;
    /*
     * The current read and write sequence numbers. Read sequence numbers are attached to requests in the order they are read from the
     * channel, and then transferred to responses. A response is not written to the channel context until its sequence number matches the
     * current write sequence, implying that all preceding messages have been written.
     */
    private int readSequence;
    private int writeSequence;

    public HttpPipeliningAggregator(int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        this.outboundHoldingQueue = new PriorityQueue<>(1);
    }

    public <Request> HttpPipelinedRequest<Request> read(final Request request) {
        return new HttpPipelinedRequest<>(readSequence++, request);
    }

    public ArrayList<HttpPipelinedResponse<Response, Listener>> write(final HttpPipelinedResponse<Response, Listener> response) {
        if (outboundHoldingQueue.size() < maxEventsHeld) {
            ArrayList<HttpPipelinedResponse<Response, Listener>> readyResponses = new ArrayList<>();
            outboundHoldingQueue.add(response);
            while (!outboundHoldingQueue.isEmpty()) {
                /*
                 * Since the response with the lowest sequence number is the top of the priority queue, we know if its sequence
                 * number does not match the current write sequence number then we have not processed all preceding responses yet.
                 */
                final HttpPipelinedResponse<Response, Listener> top = outboundHoldingQueue.peek();

                if (top.getSequence() != writeSequence) {
                    break;
                }
                outboundHoldingQueue.poll();
                readyResponses.add(top);
                writeSequence++;
            }

            return readyResponses;
        } else {
            int eventCount = outboundHoldingQueue.size() + 1;
            throw new IllegalStateException("Too many pipelined events [" + eventCount + "]. Max events allowed ["
                + maxEventsHeld + "].");
        }
    }

    public List<HttpPipelinedResponse<Response, Listener>> removeAllInflightResponses() {
        ArrayList<HttpPipelinedResponse<Response, Listener>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }
}
