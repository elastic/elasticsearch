/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.breaker;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Exception thrown when the circuit breaker trips
 */
public class CircuitBreakingException extends ElasticsearchException {

    private final long bytesWanted;
    private final long byteLimit;
    private final CircuitBreaker.Durability durability;

    public CircuitBreakingException(StreamInput in) throws IOException {
        super(in);
        byteLimit = in.readLong();
        bytesWanted = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            durability = in.readEnum(CircuitBreaker.Durability.class);
        } else {
            durability = CircuitBreaker.Durability.PERMANENT;
        }
    }

    public CircuitBreakingException(String message, CircuitBreaker.Durability durability) {
        this(message, 0, 0, durability);
    }

    public CircuitBreakingException(String message, long bytesWanted, long byteLimit, CircuitBreaker.Durability durability) {
        super(message);
        this.bytesWanted = bytesWanted;
        this.byteLimit = byteLimit;
        this.durability = durability;
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeLong(byteLimit);
        out.writeLong(bytesWanted);
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeEnum(durability);
        }
    }

    public long getBytesWanted() {
        return this.bytesWanted;
    }

    public long getByteLimit() {
        return this.byteLimit;
    }

    public CircuitBreaker.Durability getDurability() {
        return durability;
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("bytes_wanted", bytesWanted);
        builder.field("bytes_limit", byteLimit);
        builder.field("durability", durability);
    }
}
