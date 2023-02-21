/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutEngineAction extends ActionType<PutEngineAction.Response> {

    public static final PutEngineAction INSTANCE = new PutEngineAction();
    public static final String NAME = "cluster:admin/engine/put";

    public PutEngineAction() {
        super(NAME, PutEngineAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final String engineId;
        private final BytesReference content;

        private final XContentType contentType;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.engineId = in.readString();
            this.content = in.readBytesReference();
            this.contentType = in.readEnum(XContentType.class);
        }

        public Request(String engineId, BytesReference content, XContentType contentType) {
            this.engineId = engineId;
            this.content = content;
            this.contentType = contentType;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            Map<String, Object> sourceMap = XContentHelper.convertToMap(content, false, contentType).v2();
            if (sourceMap.containsKey("indices") == false) {
                validationException = addValidationError("indices are missing", validationException);
            } else {
                if (sourceMap.get("indices") instanceof Map == false) {
                    validationException = addValidationError("indices should be an array", validationException);
                }
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(engineId);
            out.writeBytesReference(content);
            XContentHelper.writeTo(out, contentType);
        }

        public String getEngineId() {
            return engineId;
        }

        public BytesReference getContent() {
            return content;
        }

        public XContentType getContentType() {
            return contentType;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        final DocWriteResponse.Result result;

        public Response(StreamInput in) throws IOException {
            super(in);
            result = DocWriteResponse.Result.readFrom(in);
        }

        public Response(DocWriteResponse.Result result) {
            this.result = result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.result.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", this.result.getLowercase());
            builder.endObject();
            return builder;
        }
    }

}
