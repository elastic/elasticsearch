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
package org.elasticsearch.common.breaker;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception thrown when the circuit breaker trips
 */
public class CircuitBreakingException extends ElasticsearchException {

    private final long bytesWanted;
    private final long byteLimit;

    public CircuitBreakingException(String message) {
        super(message);
        this.bytesWanted = 0;
        this.byteLimit = 0;
    }

    public CircuitBreakingException(StreamInput in) throws IOException {
        super(in);
        byteLimit = in.readLong();
        bytesWanted = in.readLong();
    }

    public CircuitBreakingException(String message, long bytesWanted, long byteLimit) {
        super(message);
        this.bytesWanted = bytesWanted;
        this.byteLimit = byteLimit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(byteLimit);
        out.writeLong(bytesWanted);
    }

    public long getBytesWanted() {
        return this.bytesWanted;
    }

    public long getByteLimit() {
        return this.byteLimit;
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("bytes_wanted", bytesWanted);
        builder.field("bytes_limit", byteLimit);
    }
}
