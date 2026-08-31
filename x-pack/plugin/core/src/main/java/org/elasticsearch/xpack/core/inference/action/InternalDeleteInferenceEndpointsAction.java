/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Internal action to delete inference endpoints. This should only be used internally and not exposed via a REST API.
 * For the exposed REST API action see {@link DeleteInferenceEndpointAction}.
 */
public class InternalDeleteInferenceEndpointsAction extends ActionType<AcknowledgedResponse> {

    public static final InternalDeleteInferenceEndpointsAction INSTANCE = new InternalDeleteInferenceEndpointsAction();
    public static final String NAME = "cluster:internal/xpack/inference/delete_endpoints";

    public InternalDeleteInferenceEndpointsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private final Set<String> inferenceEntityIds;

        public Request(Set<String> inferenceEntityIds, TimeValue timeout) {
            super(timeout, DEFAULT_ACK_TIMEOUT);
            this.inferenceEntityIds = Objects.requireNonNull(inferenceEntityIds);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            inferenceEntityIds = in.readCollectionAsImmutableSet(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(inferenceEntityIds);
        }

        public Set<String> getInferenceEntityIds() {
            return inferenceEntityIds;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            var request = (Request) o;
            return Objects.equals(inferenceEntityIds, request.inferenceEntityIds);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(inferenceEntityIds);
        }
    }
}
