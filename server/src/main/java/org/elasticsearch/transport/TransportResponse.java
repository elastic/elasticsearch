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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class TransportResponse extends TransportMessage {

    /**
     * Constructs a new empty transport response
     */
    public TransportResponse() {
    }

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
