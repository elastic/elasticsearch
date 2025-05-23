/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * A request class to get a status update of the async search request
 */
public class GetAsyncStatusRequest extends LegacyActionRequest {
    private final String id;
    private TimeValue keepAlive = TimeValue.MINUS_ONE;

    /**
     * Creates a new request
     *
     * @param id The id of the search progress request.
     */
    public GetAsyncStatusRequest(String id) {
        this.id = id;
    }

    public GetAsyncStatusRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            this.keepAlive = in.readTimeValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeTimeValue(keepAlive);
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
     * @param timeValue Extends the amount of time after which the result will expire (defaults to no extension).
     * @return this object
     */
    public GetAsyncStatusRequest setKeepAlive(TimeValue timeValue) {
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
        GetAsyncStatusRequest request = (GetAsyncStatusRequest) o;
        return Objects.equals(id, request.id) && keepAlive.equals(request.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, keepAlive);
    }
}
