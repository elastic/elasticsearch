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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.dictionary.CustomDictionaryService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PutCustomDictionaryAction extends ActionType<PutCustomDictionaryAction.Response> {

    public static final PutCustomDictionaryAction INSTANCE = new PutCustomDictionaryAction();
    public static final String NAME = "cluster:admin/dictionary/put";

    public PutCustomDictionaryAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private final String id;
        private final BytesReference dictionary;

        public Request(String id, BytesReference dictionary) {
            this.id = id;
            this.dictionary = dictionary;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.dictionary = in.readBytesReference();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isEmpty(id)) {
                validationException = ValidateActions.addValidationError("dictionary id must be specified", validationException);
            }
            if (dictionary == null || dictionary.length() == 0) {
                validationException = ValidateActions.addValidationError("dictionary content must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBytesReference(dictionary);
        }

        public String id() {
            return id;
        }

        public BytesReference dictionary() {
            return dictionary;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(id, request.id) && Objects.equals(dictionary, request.dictionary);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, dictionary);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final CustomDictionaryService.DictionaryOperationResult result;

        public Response(CustomDictionaryService.DictionaryOperationResult result) {
            this.result = result;
        }

        public Response(StreamInput in) throws IOException {
            this.result = in.readEnum(CustomDictionaryService.DictionaryOperationResult.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", result.name().toLowerCase());
            builder.endObject();
            return builder;
        }

        public CustomDictionaryService.DictionaryOperationResult result() {
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return result == response.result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }
}
