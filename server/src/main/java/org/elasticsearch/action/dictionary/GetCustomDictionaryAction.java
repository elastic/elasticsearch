/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.dictionary;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetCustomDictionaryAction extends ActionType<GetCustomDictionaryAction.Response> {

    public static final GetCustomDictionaryAction INSTANCE = new GetCustomDictionaryAction();
    public static final String NAME = "cluster:admin/dictionary/get";

    public GetCustomDictionaryAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private final String id;

        public Request(String id) {
            this.id = id;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isEmpty(id)) {
                validationException = ValidateActions.addValidationError("dictionary id must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        public String id() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(id, request.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final String id;
        private final String dictionary;

        public Response(String id, String dictionary) {
            this.id = id;
            this.dictionary = dictionary;
        }

        public Response(StreamInput in) throws IOException {
            this.id = in.readString();
            this.dictionary = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeString(dictionary);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("id", id);
            builder.field("dictionary", dictionary);
            builder.endObject();
            return builder;
        }

        public String id() {
            return id;
        }

        public String dictionary() {
            return dictionary;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(id, response.id) && Objects.equals(dictionary, response.dictionary);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, dictionary);
        }
    }
}
