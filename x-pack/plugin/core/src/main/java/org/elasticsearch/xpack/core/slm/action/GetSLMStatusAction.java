/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.io.IOException;

public class GetSLMStatusAction extends ActionType<GetSLMStatusAction.Response> {
    public static final GetSLMStatusAction INSTANCE = new GetSLMStatusAction();
    public static final String NAME = "cluster:admin/slm/status";

    protected GetSLMStatusAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final OperationMode mode;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.mode = in.readEnum(OperationMode.class);
        }

        public Response(OperationMode mode) {
            this.mode = mode;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this.mode);
        }

        public OperationMode getOperationMode() {
            return this.mode;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("operation_mode", this.mode);
            builder.endObject();
            return builder;
        }
    }
}
