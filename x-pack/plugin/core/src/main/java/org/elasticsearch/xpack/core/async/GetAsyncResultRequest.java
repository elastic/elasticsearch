/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class GetAsyncResultRequest extends LegacyActionRequest {
    private final String id;
    private TimeValue waitForCompletionTimeout = TimeValue.MINUS_ONE;
    private TimeValue keepAlive = TimeValue.MINUS_ONE;
    private boolean returnPartialResults = true;

    private static final TransportVersion RETURN_PARTIAL_RESULTS_VERSION = TransportVersion.fromName(
        "return_async_partial_results_query_param"
    );

    /**
     * Creates a new request
     *
     * @param id The id of the search progress request.
     */
    public GetAsyncResultRequest(String id) {
        this.id = id;
    }

    public GetAsyncResultRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.waitForCompletionTimeout = TimeValue.timeValueMillis(in.readLong());
        this.keepAlive = in.readTimeValue();
        if (in.getTransportVersion().supports(RETURN_PARTIAL_RESULTS_VERSION)) {
            this.returnPartialResults = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeLong(waitForCompletionTimeout.millis());
        out.writeTimeValue(keepAlive);
        if (out.getTransportVersion().supports(RETURN_PARTIAL_RESULTS_VERSION)) {
            out.writeBoolean(returnPartialResults);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Returns the id of the async search.
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the minimum time that the request should wait before returning a partial result (defaults to no wait).
     */
    public GetAsyncResultRequest setWaitForCompletionTimeout(TimeValue timeValue) {
        this.waitForCompletionTimeout = timeValue;
        return this;
    }

    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    /**
     * Extends the amount of time after which the result will expire (defaults to no extension).
     */
    public GetAsyncResultRequest setKeepAlive(TimeValue timeValue) {
        this.keepAlive = timeValue;
        return this;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Sets whether partial results should be returned if the search is not yet complete.
     */
    public GetAsyncResultRequest setReturnPartialResults(boolean returnPartialResults) {
        this.returnPartialResults = returnPartialResults;
        return this;
    }

    public boolean getReturnPartialResults() {
        return returnPartialResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetAsyncResultRequest request = (GetAsyncResultRequest) o;
        return Objects.equals(id, request.id)
            && waitForCompletionTimeout.equals(request.waitForCompletionTimeout)
            && keepAlive.equals(request.keepAlive)
            && returnPartialResults == request.returnPartialResults;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, waitForCompletionTimeout, keepAlive, returnPartialResults);
    }
}
