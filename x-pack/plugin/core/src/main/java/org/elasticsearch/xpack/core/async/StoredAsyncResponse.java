/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal class for temporary storage of eql search results
 */
public class StoredAsyncResponse<R extends Writeable> extends ActionResponse
    implements AsyncResponse<StoredAsyncResponse<R>>, ToXContentObject {
    private final R response;
    private final Exception exception;
    private final long expirationTimeMillis;

    public StoredAsyncResponse(R response, long expirationTimeMillis) {
        this(response, null, expirationTimeMillis);
    }

    public StoredAsyncResponse(Exception exception, long expirationTimeMillis) {
        this(null, exception, expirationTimeMillis);
    }

    public StoredAsyncResponse(Writeable.Reader<R> reader, StreamInput input) throws IOException {
        expirationTimeMillis = input.readLong();
        this.response = input.readOptionalWriteable(reader);
        this.exception = input.readException();
    }

    private StoredAsyncResponse(R response, Exception exception, long expirationTimeMillis) {
        this.response = response;
        this.exception = exception;
        this.expirationTimeMillis = expirationTimeMillis;
    }

    @Override
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    @Override
    public StoredAsyncResponse<R> withExpirationTime(long expirationTimeMillis) {
        return new StoredAsyncResponse<>(this.response, this.exception, expirationTimeMillis);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(expirationTimeMillis);
        out.writeOptionalWriteable(response);
        out.writeException(exception);
    }

    public R getResponse() {
        return response;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredAsyncResponse<?> response1 = (StoredAsyncResponse<?>) o;
        if (exception != null && response1.exception != null) {
            if (Objects.equals(exception.getClass(), response1.exception.getClass()) == false ||
                Objects.equals(exception.getMessage(), response1.exception.getMessage()) == false) {
                return false;
            }
        } else {
            if (Objects.equals(exception, response1.exception) == false) {
                return false;
            }
        }
        return expirationTimeMillis == response1.expirationTimeMillis &&
            Objects.equals(response, response1.response);
    }

    @Override
    public int hashCode() {
        return Objects.hash(response, exception == null ? null : exception.getClass(),
            exception == null ? null : exception.getMessage(), expirationTimeMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
