/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.search;

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

public class RestOpenPointInTimeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "open_point_in_time";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_pit"),
            new Route(POST, "/_pit"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, OpenPointInTimeRequest.DEFAULT_INDICES_OPTIONS);
        final String routing = request.param("routing");
        final String preference = request.param("preference");
        final TimeValue keepAlive = TimeValue.parseTimeValue(request.param("keep_alive"), null, "keep_alive");
        final OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(indices, indicesOptions, keepAlive, routing, preference);
        return channel -> client.execute(OpenPointInTimeAction.INSTANCE, openRequest, new RestToXContentListener<>(channel));
    }
}
