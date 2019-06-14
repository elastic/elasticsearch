/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class StartReindexJobAction extends Action<StartReindexJobAction.Response> {

    public static final StartReindexJobAction INSTANCE = new StartReindexJobAction();
    // TODO: Name
    public static final String NAME = "indices:admin/reindex/start_reindex";
//    public static final String NAME = "cluster:admin/reindex/start_reindex";
//    public static final String NAME = "indices:data/reindex/start_reindex";

    private StartReindexJobAction() {
        super(NAME);
    }

    @Override
    public StartReindexJobAction.Response newResponse() {
        return new StartReindexJobAction.Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private ReindexRequest reindexRequest;
        private boolean waitForCompletion = false;

        public Request() {

        }

        public Request(ReindexRequest reindexRequest) {
            this.reindexRequest = reindexRequest;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            reindexRequest = new ReindexRequest(in);
            waitForCompletion = in.readBoolean();
        }

//        public static Request fromXContent(final XContentParser parser, final String id) throws IOException {
//            Request request = new Request();
//            request.setReindexJob(ReindexJob.fromXContent(parser));
//            return request;
//        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            reindexRequest = new ReindexRequest(in);
            waitForCompletion = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            reindexRequest.writeTo(out);
            out.writeBoolean(waitForCompletion);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        public ReindexRequest getReindexRequest() {
            return reindexRequest;
        }

        public void setWaitForCompletion(boolean waitForCompletion) {
            this.waitForCompletion = waitForCompletion;
        }

        public boolean getWaitForCompletion() {
            return waitForCompletion;
        }
    }

    public static class Response extends AcknowledgedResponse {

        static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
        static final ParseField TASK_ID = new ParseField("task_id");
        static final ParseField REINDEX_RESPONSE = new ParseField("reindex_response");

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "start_reindex_response", true, args -> new Response((boolean) args[0], (String) args[1], (BulkByScrollResponse) args[2]));

        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ACKNOWLEDGED);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TASK_ID);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (parser, context) -> BulkByScrollResponse.fromXContent(parser), REINDEX_RESPONSE);
        }

        private String taskId;
        private BulkByScrollResponse reindexResponse;

        public Response() {
            super();
        }

        public Response(boolean acknowledged, String taskId) {
            super(acknowledged);
            this.taskId = taskId;
        }

        public Response(boolean acknowledged, String taskId, BulkByScrollResponse reindexResponse) {
            super(acknowledged);
            this.taskId = taskId;
            this.reindexResponse = reindexResponse;
        }

        public Response(StreamInput in) throws IOException {
            readFrom(in);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            taskId = in.readString();
            reindexResponse = in.readOptionalWriteable((input) -> {
                BulkByScrollResponse bulkByScrollResponse = new BulkByScrollResponse();
                bulkByScrollResponse.readFrom(input);
                return bulkByScrollResponse;
            });
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeOptionalWriteable(reindexResponse);
        }

        public String getTaskId() {
            return taskId;
        }

        public BulkByScrollResponse getReindexResponse() {
            return reindexResponse;
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
