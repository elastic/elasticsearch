/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationTemplate;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetSearchApplicationAction extends ActionType<GetSearchApplicationAction.Response> {

    public static final GetSearchApplicationAction INSTANCE = new GetSearchApplicationAction();
    public static final String NAME = "cluster:admin/xpack/application/search_application/get";

    private GetSearchApplicationAction() {
        super(NAME, GetSearchApplicationAction.Response::new);
    }

    public static class Request extends ActionRequest {
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
                validationException = addValidationError("name missing", validationException);
            }

            return validationException;
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SearchApplication searchApp;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.searchApp = new SearchApplication(in);
        }

        public Response(SearchApplication app) {
            this.searchApp = app;
        }

        public Response(
            String name,
            String[] indices,
            String analyticsCollectionName,
            long updatedAtMillis,
            SearchApplicationTemplate template
        ) {
            this.searchApp = new SearchApplication(name, indices, analyticsCollectionName, updatedAtMillis, template);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            searchApp.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return searchApp.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(searchApp, response.searchApp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(searchApp);
        }
    }
}
