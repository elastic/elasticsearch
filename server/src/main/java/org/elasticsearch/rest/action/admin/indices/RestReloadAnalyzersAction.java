/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzerAction;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestReloadAnalyzersAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_reload_search_analyzers"), new Route(POST, "/{index}/_reload_search_analyzers"));
    }

    @Override
    public String getName() {
        return "reload_search_analyzers_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ReloadAnalyzersRequest reloadAnalyzersRequest = new ReloadAnalyzersRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        reloadAnalyzersRequest.indicesOptions(IndicesOptions.fromRequest(request, reloadAnalyzersRequest.indicesOptions()));
        return channel -> client.execute(ReloadAnalyzerAction.INSTANCE, reloadAnalyzersRequest, new RestToXContentListener<>(channel));
    }
}
