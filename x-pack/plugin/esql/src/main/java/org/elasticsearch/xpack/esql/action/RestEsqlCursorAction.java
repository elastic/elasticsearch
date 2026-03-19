/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlCursorAction extends BaseRestHandler {

    private static final ObjectParser<CursorRequestBody, Void> PARSER = new ObjectParser<>("esql/cursor", false, CursorRequestBody::new);
    static {
        PARSER.declareString(CursorRequestBody::setCursor, new ParseField("cursor"));
        PARSER.declareField(
            CursorRequestBody::setKeepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), "keep_alive"),
            new ParseField("keep_alive"),
            ObjectParser.ValueType.VALUE
        );
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/cursor"), new Route(DELETE, "/_query/cursor/{cursor}"));
    }

    @Override
    public String getName() {
        return "esql_cursor";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method() == DELETE) {
            return prepareDeleteRequest(request, client);
        }
        return prepareNextPageRequest(request, client);
    }

    private RestChannelConsumer prepareNextPageRequest(RestRequest request, NodeClient client) throws IOException {
        CursorRequestBody body;
        try (XContentParser parser = request.contentParser()) {
            body = PARSER.parse(parser, null);
        }
        EsqlCursorRequest cursorRequest = new EsqlCursorRequest(body.cursor, body.keepAlive);
        return channel -> client.execute(EsqlCursorAction.INSTANCE, cursorRequest, new EsqlResponseListener(channel, request));
    }

    private RestChannelConsumer prepareDeleteRequest(RestRequest request, NodeClient client) {
        String cursorToken = request.param("cursor");
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        EsqlDeleteCursorRequest deleteRequest = new EsqlDeleteCursorRequest(cursorToken, waitForCompletion);
        return channel -> client.execute(EsqlDeleteCursorAction.INSTANCE, deleteRequest, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, DROP_NULL_COLUMNS_OPTION);
    }

    private static class CursorRequestBody {
        private String cursor;
        @Nullable
        private TimeValue keepAlive;

        void setCursor(String cursor) {
            this.cursor = cursor;
        }

        void setKeepAlive(TimeValue keepAlive) {
            this.keepAlive = keepAlive;
        }
    }
}
