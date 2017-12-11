/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class SqlClearCursorAction
        extends Action<SqlClearCursorAction.Request, SqlClearCursorAction.Response, SqlClearCursorAction.RequestBuilder> {

    public static final SqlClearCursorAction INSTANCE = new SqlClearCursorAction();
    public static final String NAME = "indices:data/read/sql/close_cursor";

    private SqlClearCursorAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        public static final ParseField CURSOR = new ParseField("cursor");

        static {
            PARSER.declareString((request, nextPage) -> request.setCursor(Cursor.decodeFromString(nextPage)), CURSOR);
        }

        private Cursor cursor;

        public Request() {

        }

        public Request(Cursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (getCursor() == null) {
                validationException = addValidationError("cursor is required", validationException);
            }
            return validationException;
        }

        public Cursor getCursor() {
            return cursor;
        }

        public Request setCursor(Cursor cursor) {
            this.cursor = cursor;
            return this;
        }

        @Override
        public String getDescription() {
            return "SQL Clean cursor [" + getCursor() + "]";
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            cursor = in.readNamedWriteable(Cursor.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeNamedWriteable(cursor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(cursor, request.cursor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cursor);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {
        public RequestBuilder(ElasticsearchClient client, SqlClearCursorAction action, Cursor cursor) {
            super(client, action, new Request(cursor));
        }

        public RequestBuilder(ElasticsearchClient client, SqlClearCursorAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder cursor(Cursor cursor) {
            request.setCursor(cursor);
            return this;
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private static final ParseField SUCCEEDED = new ParseField("succeeded");

        private boolean succeeded;

        public Response(boolean succeeded) {
            this.succeeded = succeeded;
        }

        Response() {
        }

        /**
         * @return Whether the attempt to clear a cursor was successful.
         */
        public boolean isSucceeded() {
            return succeeded;
        }

        public Response setSucceeded(boolean succeeded) {
            this.succeeded = succeeded;
            return this;
        }

        @Override
        public RestStatus status() {
            return succeeded ? NOT_FOUND : OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SUCCEEDED.getPreferredName(), succeeded);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            succeeded = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(succeeded);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return succeeded == response.succeeded;
        }

        @Override
        public int hashCode() {
            return Objects.hash(succeeded);
        }
    }


    public static class TransportAction extends HandledTransportAction<Request, Response> {
        private final PlanExecutor planExecutor;
        private final SqlLicenseChecker sqlLicenseChecker;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool,
                                    TransportService transportService, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                    PlanExecutor planExecutor,
                                    SqlLicenseChecker sqlLicenseChecker) {
            super(settings, NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.planExecutor = planExecutor;
            this.sqlLicenseChecker = sqlLicenseChecker;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            sqlLicenseChecker.checkIfSqlAllowed();
            Cursor cursor = request.getCursor();
            planExecutor.cleanCursor(Configuration.DEFAULT, cursor, ActionListener.wrap(
                    success -> listener.onResponse(new Response(success)), listener::onFailure));
        }
    }

    public static class RestAction extends BaseRestHandler {
        public RestAction(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(POST, "/_xpack/sql/close", this);
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            Request sqlRequest;
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                sqlRequest = Request.PARSER.apply(parser, null);
            }
            return channel -> client.executeLocally(SqlClearCursorAction.INSTANCE, sqlRequest, new RestToXContentListener<>(channel));
        }

        @Override
        public String getName() {
            return "sql_translate_action";
        }
    }
}
