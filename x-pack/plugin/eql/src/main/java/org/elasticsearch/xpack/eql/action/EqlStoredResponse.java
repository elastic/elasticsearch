/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.async.AsyncResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal class for temporary storage of eql search results
 */
public class EqlStoredResponse implements AsyncResponse<EqlStoredResponse> {
    private final EqlSearchResponse response;
    private final Exception exception;
    private final long expirationTimeMillis;

    public EqlStoredResponse(EqlSearchResponse response, long expirationTimeMillis) {
        this(response, null, expirationTimeMillis);
    }

    public EqlStoredResponse(Exception exception, long expirationTimeMillis) {
        this(null, exception, expirationTimeMillis);
    }

    public EqlStoredResponse(StreamInput input) throws IOException {
        expirationTimeMillis = input.readLong();
        this.response = input.readOptionalWriteable(EqlSearchResponse::new);
        this.exception = input.readException();
    }

    private EqlStoredResponse(EqlSearchResponse response, Exception exception, long expirationTimeMillis) {
        this.response = response;
        this.exception = exception;
        this.expirationTimeMillis = expirationTimeMillis;
    }

    @Override
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    @Override
    public EqlStoredResponse withExpirationTime(long expirationTimeMillis) {
        return new EqlStoredResponse(this.response, this.exception, expirationTimeMillis);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(expirationTimeMillis);
        out.writeOptionalWriteable(response);
        out.writeException(exception);
    }

    public EqlSearchResponse getResponse() {
        return response;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EqlStoredResponse response1 = (EqlStoredResponse) o;
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
}
