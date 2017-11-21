/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_FETCH_SIZE;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_PAGE_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_REQUEST_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_TIME_ZONE;

public class SqlTranslateAction
        extends Action<SqlTranslateAction.Request, SqlTranslateAction.Response, SqlTranslateAction.RequestBuilder> {

    public static final SqlTranslateAction INSTANCE = new SqlTranslateAction();
    public static final String NAME = "indices:data/read/sql/translate";

    private SqlTranslateAction() {
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

    public static class Request extends AbstractSqlRequest {
        public static final ObjectParser<Request, Void> PARSER = objectParser(Request::new);

        public Request() {}

        public Request(String query, QueryBuilder filter, DateTimeZone timeZone, int fetchSize, TimeValue requestTimeout,
                       TimeValue pageTimeout) {
            super(query, filter, timeZone, fetchSize, requestTimeout, pageTimeout);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if ((false == Strings.hasText(query()))) {
                validationException = addValidationError("query is required", validationException);
            }
            return validationException;
        }

        @Override
        public String getDescription() {
            return "SQL Translate [" + query() + "][" + filter() + "]";
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {
        public RequestBuilder(ElasticsearchClient client, SqlTranslateAction action) {
            this(client, action, null, null, DEFAULT_TIME_ZONE, DEFAULT_FETCH_SIZE, DEFAULT_REQUEST_TIMEOUT, DEFAULT_PAGE_TIMEOUT);
        }

        public RequestBuilder(ElasticsearchClient client, SqlTranslateAction action, String query, QueryBuilder filter,
                              DateTimeZone timeZone, int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout) {
            super(client, action, new Request(query, filter, timeZone, fetchSize, requestTimeout, pageTimeout));
        }

        public RequestBuilder query(String query) {
            request.query(query);
            return this;
        }

        public RequestBuilder timeZone(DateTimeZone timeZone) {
            request.timeZone(timeZone);
            return this;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private SearchSourceBuilder source;

        public Response() {
        }

        public Response(SearchSourceBuilder source) {
            this.source = source;
        }

        public SearchSourceBuilder source() {
            return source;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            source = new SearchSourceBuilder(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            source.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Response other = (Response) obj;
            return Objects.equals(source, other.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return source.toXContent(builder, params);
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
            super(settings, SqlTranslateAction.NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);

            this.planExecutor = planExecutor;
            this.sqlLicenseChecker = sqlLicenseChecker;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            sqlLicenseChecker.checkIfSqlAllowed();
            String query = request.query();

            Configuration cfg = new Configuration(request.timeZone(), request.fetchSize(),
                    request.requestTimeout(), request.pageTimeout(), request.filter());

            listener.onResponse(new Response(planExecutor.searchSource(query, cfg)));
        }
    }

    public static class RestAction extends BaseRestHandler {
        public RestAction(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(GET, "/_sql/translate", this);
            controller.registerHandler(POST, "/_sql/translate", this);
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            Request sqlRequest;
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                sqlRequest = Request.PARSER.apply(parser, null);
            }

            return channel -> client.executeLocally(SqlTranslateAction.INSTANCE,
                    sqlRequest, new RestToXContentListener<Response>(channel));
        }

        @Override
        public String getName() {
            return "sql_translate_action";
        }
    }
}
