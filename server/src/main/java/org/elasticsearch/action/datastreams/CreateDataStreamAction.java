/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class CreateDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final CreateDataStreamAction INSTANCE = new CreateDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/create";

    private CreateDataStreamAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String name;
        private final long startTime;

        public Request(String name) {
            this.name = name;
            this.startTime = System.currentTimeMillis();
        }

        public Request(String name, long startTime) {
            this.name = name;
            this.startTime = startTime;
        }

        public String getName() {
            return name;
        }

        public long getStartTime() {
            return startTime;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(name) == false) {
                validationException = ValidateActions.addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
                this.startTime = in.readVLong();
            } else {
                this.startTime = System.currentTimeMillis();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
                out.writeVLong(startTime);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) && startTime == request.startTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, startTime);
        }

        @Override
        public String[] indices() {
            return new String[] { name };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

}
