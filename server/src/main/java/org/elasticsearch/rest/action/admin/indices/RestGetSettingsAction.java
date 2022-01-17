/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetSettingsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_settings"),
            new Route(GET, "/_settings/{name}"),
            new Route(GET, "/{index}/_settings"),
            new Route(GET, "/{index}/_settings/{name}"),
            new Route(GET, "/{index}/_setting/{name}")
        );
    }

    @Override
    public String getName() {
        return "get_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] names = request.paramAsStringArrayOrEmptyIfAll("name");
        final boolean renderDefaults = request.paramAsBoolean("include_defaults", false);
        // This is required so the "flat_settings" parameter counts as consumed
        request.paramAsBoolean("flat_settings", false);
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(Strings.splitStringByCommaToArray(request.param("index")))
            .indicesOptions(IndicesOptions.fromRequest(request, IndicesOptions.strictExpandOpen()))
            .humanReadable(request.hasParam("human"))
            .includeDefaults(renderDefaults)
            .names(names);
        getSettingsRequest.local(request.paramAsBoolean("local", getSettingsRequest.local()));
        getSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getSettingsRequest.masterNodeTimeout()));
        return channel -> client.admin().indices().getSettings(getSettingsRequest, new RestToXContentListener<>(channel));
    }
}
