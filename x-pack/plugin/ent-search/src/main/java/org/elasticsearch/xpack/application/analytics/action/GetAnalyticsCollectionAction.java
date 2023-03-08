/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;
import org.elasticsearch.xpack.application.analytics.action.support.BaseAnalyticsCollectionResponse;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetAnalyticsCollectionAction extends ActionType<GetAnalyticsCollectionAction.Response> {

    public static final GetAnalyticsCollectionAction INSTANCE = new GetAnalyticsCollectionAction();
    public static final String NAME = "cluster:admin/xpack/application/analytics/get";

    private GetAnalyticsCollectionAction() {
        super(NAME, GetAnalyticsCollectionAction.Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {
        private final String collectionName;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.collectionName = in.readString();
        }

        public Request(String collectionName) {
            this.collectionName = collectionName;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(collectionName)) {
                validationException = addValidationError("collection name missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(collectionName);
        }

        public String getCollectionName() {
            return this.collectionName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.collectionName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(this.collectionName, request.collectionName);
        }
    }

    public static class Response extends BaseAnalyticsCollectionResponse implements ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(AnalyticsCollection analyticsCollection) {
            super(analyticsCollection);
        }

        public Response(String collectionName) {
            super(new AnalyticsCollection(collectionName));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return toXContentCommon(builder, params);
        }

    }

}
