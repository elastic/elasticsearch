/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndex;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetAiIndexAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/ai_index/get";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private GetAiIndexAction() {/* no instances */}

    public static class Request extends UntypedActionRequest {

        private final String space;
        private final String id;

        public Request(String space, String id) {
            this.space = space;
            this.id = id;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.space = in.readString();
            this.id = in.readString();
        }

        public String space() {
            return space;
        }

        public String id() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(space);
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return validateSpaceAndId(space, id);
        }

        static ActionRequestValidationException validateSpaceAndId(String space, String id) {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(space)) {
                validationException = addValidationError("[space] cannot be null or empty", validationException);
            }
            if (Strings.isNullOrEmpty(id)) {
                validationException = addValidationError("[id] cannot be null or empty", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(space, request.space) && Objects.equals(id, request.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(space, id);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final AiIndex aiIndex;

        public Response(AiIndex aiIndex) {
            this.aiIndex = aiIndex;
        }

        public Response(StreamInput in) throws IOException {
            this.aiIndex = new AiIndex(in);
        }

        public AiIndex aiIndex() {
            return aiIndex;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            aiIndex.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return aiIndex.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(aiIndex, response.aiIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aiIndex);
        }
    }
}
