/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutAnalyticsCollectionAction extends ActionType<PutAnalyticsCollectionAction.Response> {

    public static final PutAnalyticsCollectionAction INSTANCE = new PutAnalyticsCollectionAction();
    public static final String NAME = "cluster:admin/xpack/application/analytics/put";

    public PutAnalyticsCollectionAction() {
        super(NAME, PutAnalyticsCollectionAction.Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final String name;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        public Request(String name) {
            this.name = name;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (name == null || name.isEmpty()) {
                validationException = addValidationError("Analytics collection name is missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends AcknowledgedResponse implements StatusToXContentObject {

        public static final ParseField COLLECTION_NAME_FIELD = new ParseField("name");

        private final String name;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        public Response(boolean acknowledged, String name) {
            super(acknowledged);
            this.name = name;
        }

        @Override
        public RestStatus status() {
            return RestStatus.CREATED;
        }

        public String getName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + name.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return isAcknowledged() == response.isAcknowledged() && Objects.equals(name, response.name);
        }

        @Override
        protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
            builder.field(COLLECTION_NAME_FIELD.getPreferredName(), name);
        }
    }
}
