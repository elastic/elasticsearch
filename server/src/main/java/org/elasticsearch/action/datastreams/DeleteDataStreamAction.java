/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDataStreamAction INSTANCE = new DeleteDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/delete";

    private DeleteDataStreamAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> implements IndicesRequest.Replaceable {

        private String[] names;

        // Security intercepts requests and rewrites names if wildcards are used to expand to concrete resources
        // that a user has privileges for.
        // This keeps track whether wildcards were originally specified in names,
        // So that in the case no matches ds are found, that either an
        // empty response can be returned in case wildcards were used or
        // 404 status code returned in case no wildcard were used.
        private final boolean wildcardExpressionsOriginallySpecified;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);

        public Request(TimeValue masterNodeTimeout, String... names) {
            super(masterNodeTimeout);
            this.names = Objects.requireNonNull(names);
            this.wildcardExpressionsOriginallySpecified = Arrays.stream(names).anyMatch(Regex::isSimpleMatchPattern);
        }

        @Deprecated(forRemoval = true) // temporary compatibility shim
        public Request(String... names) {
            this(MasterNodeRequest.TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, names);
        }

        public String[] getNames() {
            return names;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (CollectionUtils.isEmpty(names)) {
                validationException = addValidationError("no data stream(s) specified", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.wildcardExpressionsOriginallySpecified = in.readBoolean();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            out.writeBoolean(wildcardExpressionsOriginallySpecified);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return wildcardExpressionsOriginallySpecified == request.wildcardExpressionsOriginallySpecified
                && Arrays.equals(names, request.names)
                && indicesOptions.equals(request.indicesOptions);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(wildcardExpressionsOriginallySpecified, indicesOptions);
            result = 31 * result + Arrays.hashCode(names);
            return result;
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public Request indicesOptions(IndicesOptions options) {
            this.indicesOptions = options;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        public boolean isWildcardExpressionsOriginallySpecified() {
            return wildcardExpressionsOriginallySpecified;
        }
    }

}
