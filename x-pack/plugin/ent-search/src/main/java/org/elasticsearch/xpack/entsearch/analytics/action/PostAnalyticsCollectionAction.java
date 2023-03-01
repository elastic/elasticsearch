/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.entsearch.analytics.AnalyticsCollection;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PostAnalyticsCollectionAction extends ActionType<PostAnalyticsCollectionAction.Response> {

    public static final PostAnalyticsCollectionAction INSTANCE = new PostAnalyticsCollectionAction();
    public static final String NAME = "cluster:behavioral_analytics";

    public PostAnalyticsCollectionAction() { super(NAME, PostAnalyticsCollectionAction.Response::new); }
    public static class Request extends ActionRequest {
        private final AnalyticsCollection analyticsCollection;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.analyticsCollection = new AnalyticsCollection(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(String resourceName, BytesReference content, XContentType contentType) {
            this.analyticsCollection = AnalyticsCollection.fromXContentBytes(resourceName, content, contentType);
        }

        public Request(AnalyticsCollection analyticsCollection) {
            this.analyticsCollection = analyticsCollection;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            analyticsCollection.writeTo(out);
        }

        public AnalyticsCollection getAnalyticsCollection() {
            return analyticsCollection;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PostAnalyticsCollectionAction.Request that = (PostAnalyticsCollectionAction.Request) o;
            return analyticsCollection.getName() == that.analyticsCollection.getName();
        }

        @Override
        public int hashCode() {
            return Objects.hash(analyticsCollection);
        }
    }

    public static class Response extends ActionResponse implements Writeable, StatusToXContentObject {
        final AnalyticsCollection analyticsCollection;

        public Response(StreamInput in) throws IOException {
            super(in);
            analyticsCollection = new AnalyticsCollection(in);
        }

        public Response(AnalyticsCollection analyticsCollection) {
            this.analyticsCollection = analyticsCollection;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            //
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", this.analyticsCollection.getName());
            builder.field("datastream_name", this.analyticsCollection.getEventDataStream());
            builder.endObject();
            return builder;
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return analyticsCollection.getName() == that.analyticsCollection.getName();
        }

        @Override
        public int hashCode() {
            return Objects.hash(analyticsCollection);
        }
    }
}
