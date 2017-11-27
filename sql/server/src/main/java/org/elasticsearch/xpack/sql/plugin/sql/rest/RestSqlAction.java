/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.rest;

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.sql.plugin.CliFormatter;
import org.elasticsearch.xpack.sql.plugin.CliFormatterCursor;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlAction extends BaseRestHandler {
    public RestSqlAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_sql", this);
        controller.registerHandler(POST, "/_sql", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlRequest.PARSER.apply(parser, null);
        }

        XContentType xContentType = XContentType.fromMediaTypeOrFormat(request.param("format", request.header("Accept")));
        if (xContentType != null) {
            // The client expects us to send back results in a XContent format
            return channel -> client.executeLocally(SqlAction.INSTANCE, sqlRequest, new RestToXContentListener<>(channel));
        }
        // The client accepts plain text
        long startNanos = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, new RestResponseListener<SqlResponse>(channel) {
            @Override
            public RestResponse buildResponse(SqlResponse response) throws Exception {
                final String data;
                final CliFormatter formatter;
                if (sqlRequest.cursor() != Cursor.EMPTY) {
                    formatter = ((CliFormatterCursor) sqlRequest.cursor()).getCliFormatter();
                    data = formatter.formatWithoutHeader(response);
                } else {
                    formatter = new CliFormatter(response);
                    data = formatter.formatWithHeader(response);
                }

                final Cursor responseCursor;
                if (response.cursor() == Cursor.EMPTY) {
                    responseCursor = Cursor.EMPTY;
                } else {
                    responseCursor = new CliFormatterCursor(response.cursor(), formatter);
                }
                return buildTextResponse(responseCursor, System.nanoTime() - startNanos, data);
            }
        });
    }

    private RestResponse buildTextResponse(Cursor responseCursor, long tookNanos, String data)
            throws IOException {
        RestResponse restResponse = new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE,
                data.getBytes(StandardCharsets.UTF_8));
        if (responseCursor != Cursor.EMPTY) {
            restResponse.addHeader("Cursor", Cursor.encodeToString(Version.CURRENT, responseCursor));
        }
        restResponse.addHeader("Took-nanos", Long.toString(tookNanos));
        return restResponse;
    }

    @Override
    public String getName() {
        return "xpack_sql_action";
    }
}

