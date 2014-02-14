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

package org.elasticsearch.benchmark.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 *
 */
public class BenchmarkMessageResponse extends TransportResponse {

    long id;
    byte[] payload;

    public BenchmarkMessageResponse(BenchmarkMessageRequest request) {
        this.id = request.id;
        this.payload = request.payload;
    }

    public BenchmarkMessageResponse(long id, byte[] payload) {
        this.id = id;
        this.payload = payload;
    }

    public BenchmarkMessageResponse() {
    }

    public long id() {
        return id;
    }

    public byte[] payload() {
        return payload;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
        payload = new byte[in.readVInt()];
        in.readFully(payload);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        out.writeVInt(payload.length);
        out.writeBytes(payload);
    }
}
