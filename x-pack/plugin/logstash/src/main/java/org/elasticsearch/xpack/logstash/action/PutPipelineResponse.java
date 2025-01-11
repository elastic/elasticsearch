/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

public class PutPipelineResponse extends ActionResponse {

    private final RestStatus status;

    public PutPipelineResponse(RestStatus status) {
        this.status = Objects.requireNonNull(status);
    }

    public PutPipelineResponse(StreamInput in) throws IOException {
        this.status = in.readEnum(RestStatus.class);
    }

    public RestStatus status() {
        return status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutPipelineResponse that = (PutPipelineResponse) o;
        return status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
