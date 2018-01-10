/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlListTablesAction extends BaseRestHandler {

    public RestSqlListTablesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, SqlListTablesAction.REST_ENDPOINT, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SqlListTablesRequest listTablesRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            listTablesRequest = SqlListTablesRequest.fromXContent(parser, AbstractSqlRequest.Mode.fromString(request.param("mode")));
        }
        return channel -> client.executeLocally(SqlListTablesAction.INSTANCE, listTablesRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xpack_sql_list_tables_action";
    }
}

