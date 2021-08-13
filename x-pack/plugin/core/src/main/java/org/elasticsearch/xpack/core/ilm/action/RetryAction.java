/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class RetryAction extends ActionType<AcknowledgedResponse> {
    public static final RetryAction INSTANCE = new RetryAction();
    public static final String NAME = "indices:admin/ilm/retry";

    protected RetryAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {
        private String[] indices = Strings.EMPTY_ARRAY;
        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        public Request(String... indices) {
            this.indices = indices;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        public Request() {
        }

        @Override
        public Request indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), indicesOptions);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.deepEquals(indices, other.indices)
                && Objects.equals(indicesOptions, other.indicesOptions);
        }

    }
}
