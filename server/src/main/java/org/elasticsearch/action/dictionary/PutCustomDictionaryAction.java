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
import org.elasticsearch.dictionary.CustomDictionaryService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class PutCustomDictionaryAction extends ActionType<PutCustomDictionaryAction.Response> {

    public static final PutCustomDictionaryAction INSTANCE = new PutCustomDictionaryAction();
    public static final String NAME = "cluster:admin/dictionary/put";

    public PutCustomDictionaryAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        public static final ParseField ID = new ParseField("id");
        public static final ParseField CONTENT = new ParseField("content");

        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "put_custom_dictionary_request",
            false,
            (args) -> new Request((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), ID);
            PARSER.declareString(constructorArg(), CONTENT);
        }

        private final String id;
        private final String content;

        public Request(String id, String content) {
            this.id = id;
            this.content = content;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.content = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrBlank(id)) {
                validationException = ValidateActions.addValidationError("dictionary id must be specified", validationException);
            }
            if (content == null) {
                validationException = ValidateActions.addValidationError("dictionary content must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeString(content);
        }

        public String id() {
            return id;
        }

        public String content() {
            return content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(id, request.id) && Objects.equals(content, request.content);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, content);
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
            builder.field("result", result.name().toLowerCase(Locale.ROOT));
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
