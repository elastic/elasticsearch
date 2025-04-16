/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.PutDataStreamSettingsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDataStreamSettingsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_data_stream_settings_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/{name}/_settings"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Settings settings;
        try (XContentParser parser = request.contentParser()) {
            settings = Settings.fromXContent(parser);
        }
        PutDataStreamSettingsAction.Request putDataStreamRequest = new PutDataStreamSettingsAction.Request(
            request.param("name"),
            settings,
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request)
        );
        return channel -> client.execute(PutDataStreamSettingsAction.INSTANCE, putDataStreamRequest, new RestToXContentListener<>(channel));
    }
}
