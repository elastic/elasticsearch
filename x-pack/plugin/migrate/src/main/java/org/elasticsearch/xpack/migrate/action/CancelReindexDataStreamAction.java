/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class CancelReindexDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final CancelReindexDataStreamAction INSTANCE = new CancelReindexDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/reindex_cancel";

    public CancelReindexDataStreamAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest {
        private final String index;

        public Request(String index) {
            super();
            this.index = index;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean getShouldStoreResult() {
            return true;
        }

        public String getIndex() {
            return index;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(index);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Request && index.equals(((Request) other).index);
        }

        public Request nodeRequest(String thisNodeId, long thisTaskId) {
            Request copy = new Request(index);
            copy.setParentTask(thisNodeId, thisTaskId);
            return copy;
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }
}
