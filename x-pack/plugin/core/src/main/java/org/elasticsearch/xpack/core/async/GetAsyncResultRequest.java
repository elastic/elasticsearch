/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class GetAsyncResultRequest extends ActionRequest {
    private final String id;
    private TimeValue waitForCompletion = TimeValue.MINUS_ONE;
    private TimeValue keepAlive = TimeValue.MINUS_ONE;

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
        this.waitForCompletion = TimeValue.timeValueMillis(in.readLong());
        this.keepAlive = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeLong(waitForCompletion.millis());
        out.writeTimeValue(keepAlive);
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
    public GetAsyncResultRequest setWaitForCompletion(TimeValue timeValue) {
        this.waitForCompletion = timeValue;
        return this;
    }

    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetAsyncResultRequest request = (GetAsyncResultRequest) o;
        return Objects.equals(id, request.id) &&
            waitForCompletion.equals(request.waitForCompletion) &&
            keepAlive.equals(request.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, waitForCompletion, keepAlive);
    }
}
