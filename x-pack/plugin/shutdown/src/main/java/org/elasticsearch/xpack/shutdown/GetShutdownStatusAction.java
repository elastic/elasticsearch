/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GetShutdownStatusAction extends ActionType<GetShutdownStatusAction.Response> {

    public static final GetShutdownStatusAction INSTANCE = new GetShutdownStatusAction();
    public static final String NAME = "cluster:admin/shutdown/get";

    public GetShutdownStatusAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String[] nodeIds;

        public Request(String... nodeIds) {
            this.nodeIds = nodeIds;
        }

        public static Request readFrom(StreamInput in) throws IOException {
            return new Request(in.readStringArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(this.nodeIds);
        }

        public String[] getNodeIds() {
            return nodeIds;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public Response(StreamInput in) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
