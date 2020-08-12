/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestOpenSearchContextAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "open_search_context";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_search_context"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, OpenSearchContextRequest.DEFAULT_INDICES_OPTIONS);
        final String routing = request.param("routing");
        final String preference = request.param("preference");
        final TimeValue keepAlive = TimeValue.parseTimeValue(request.param("keep_alive"), null, "keep_alive");
        final OpenSearchContextRequest openRequest = new OpenSearchContextRequest(indices, indicesOptions, keepAlive, routing, preference);
        return channel -> client.execute(OpenSearchContextAction.INSTANCE, openRequest, new RestToXContentListener<>(channel));
    }
}
