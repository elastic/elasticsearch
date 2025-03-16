/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class TransportResponse extends TransportMessage {

    /**
     * Constructs a new empty transport response
     */
    public TransportResponse() {}

    public static class Empty extends TransportResponse {
        public static final Empty INSTANCE = new Empty();

        private Empty() {/* singleton */}

        @Override
        public String toString() {
            return "Empty{}";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
