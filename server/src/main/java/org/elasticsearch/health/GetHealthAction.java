/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class GetHealthAction extends ActionType<GetHealthAction.Response> {

    public static final GetHealthAction INSTANCE = new GetHealthAction();
    public static final String NAME = "indices:monitor/fleet/global_checkpoints";

    private GetHealthAction() {
        super(NAME, GetHealthAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public Response(StreamInput in) {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new AssertionError("GetHealthAction should not be sent over the wire.");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            return builder.endObject();
        }
    }

    public static class Request extends ActionRequest {


        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TransportAction extends org.elasticsearch.action.support.TransportAction<Request, Response> {

        @Inject
        public TransportAction(final ActionFilters actionFilters, final TransportService transportService) {
            super(NAME, actionFilters, transportService.getTaskManager());
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        }
    }
}
