/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlTranslateRequest;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlTranslateResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlTranslateAction extends BaseRestHandler {

    public RestSqlTranslateAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_sql/translate", this);
        controller.registerHandler(POST, "/_sql/translate", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlTranslateRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlTranslateRequest.PARSER.apply(parser, null);
        }

        return channel -> client.executeLocally(SqlTranslateAction.INSTANCE, sqlRequest, new RestToXContentListener<SqlTranslateResponse>(channel));
    }

    @Override
    public String getName() {
        return "sql_translate_action";
    }
}