/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GetAutoscalingDecisionAction extends ActionType<GetAutoscalingDecisionAction.Response> {

    public static final GetAutoscalingDecisionAction INSTANCE = new GetAutoscalingDecisionAction();
    public static final String NAME = "cluster:admin/autoscaling/get_autoscaling_decision";

    private GetAutoscalingDecisionAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<GetAutoscalingDecisionAction.Request> implements ToXContentObject {

        public Request() {

        }

        public Request(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {

            }
            builder.endObject();
            return builder;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public Response() {

        }

        public Response(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(final StreamOutput out) {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {

            }
            builder.endObject();
            return builder;
        }

    }

}
