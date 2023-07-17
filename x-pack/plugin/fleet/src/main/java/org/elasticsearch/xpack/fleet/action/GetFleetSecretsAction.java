/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class GetFleetSecretsAction extends ActionType<GetFleetSecretsAction.Response> {
    public static final GetFleetSecretsAction INSTANCE = new GetFleetSecretsAction();

    public static final String NAME = "tmpfixme"; // TODO: what should this be?

    private GetFleetSecretsAction() {
        super(NAME, GetFleetSecretsAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public Response(StreamInput in) {
            throw new AssertionError("GetFleetSecretsAction should not be sent over the wire.");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetFleetSecretsAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            // TODO: builder.field, builder.array
            return builder.endObject();
        }
    }

    public static class Request extends ActionRequest { //  implements IndicesRequest
        // TODO: missing impl details

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            return e;
        }
    }

    public static class TransportAction extends org.elasticsearch.action.support.TransportAction<Request, Response> {
        // TODO: missing impl details
        public TransportAction(final ActionFilters actionFilters, final TransportService transportService) {
            super(NAME, actionFilters, transportService.getTaskManager());
        }

        protected void doExecute(
            Task task,
            GetFleetSecretsAction.Request request,
            ActionListener<GetFleetSecretsAction.Response> listener) {
            // impl
        }
    }
}
