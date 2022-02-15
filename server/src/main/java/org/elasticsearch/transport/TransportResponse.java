/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class TransportResponse extends TransportMessage {

    /**
     * Constructs a new empty transport response
     */
    public TransportResponse() {}

    /**
     * Constructs a new transport response with the data from the {@link StreamInput}. This is
     * currently a no-op. However, this exists to allow extenders to call <code>super(in)</code>
     * so that reading can mirror writing where we often call <code>super.writeTo(out)</code>.
     */
    public TransportResponse(StreamInput in) throws IOException {
        super(in);
    }

    public static class Empty extends TransportResponse {
        public static final Empty INSTANCE = new Empty();

        @Override
        public String toString() {
            return "Empty{}";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
