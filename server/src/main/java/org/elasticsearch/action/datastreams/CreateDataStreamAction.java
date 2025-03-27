/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class CreateDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final CreateDataStreamAction INSTANCE = new CreateDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/create";

    private CreateDataStreamAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String name;
        private final long startTime;
        private final ComposableIndexTemplate indexTemplate;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
            this(masterNodeTimeout, ackTimeout, name, null);
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name, ComposableIndexTemplate indexTemplate) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.indexTemplate = indexTemplate;
            this.startTime = System.currentTimeMillis();
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name, long startTime) {
            this(masterNodeTimeout, ackTimeout, name, null, startTime);
        }

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String name,
            ComposableIndexTemplate indexTemplate,
            long startTime
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.indexTemplate = indexTemplate;
            this.startTime = startTime;
        }

        public String getName() {
            return name;
        }

        public ComposableIndexTemplate getIndexTemplate() {
            return indexTemplate;
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
            this.startTime = in.readVLong();
            if (in.getTransportVersion().onOrAfter(TransportVersions.TEMPLATES_IN_DATA_STREAMS)) {
                this.indexTemplate = in.readOptionalWriteable(ComposableIndexTemplate::new);
            } else {
                this.indexTemplate = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeVLong(startTime);
            if (out.getTransportVersion().onOrAfter(TransportVersions.TEMPLATES_IN_DATA_STREAMS)) {
                out.writeOptionalWriteable(indexTemplate);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) && startTime == request.startTime && Objects.equals(indexTemplate, request.indexTemplate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, startTime, indexTemplate);
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
