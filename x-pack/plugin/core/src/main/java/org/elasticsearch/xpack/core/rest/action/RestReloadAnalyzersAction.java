/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.ReloadAnalyzerAction;
import org.elasticsearch.xpack.core.action.ReloadAnalyzersRequest;

import java.io.IOException;
public class RestReloadAnalyzersAction extends BaseRestHandler {

    public RestReloadAnalyzersAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_reload_search_analyzers", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_reload_search_analyzers", this);
    }

    @Override
    public String getName() {
        return "reload_search_analyzers_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ReloadAnalyzersRequest reloadAnalyzersRequest = new ReloadAnalyzersRequest(
                Strings.splitStringByCommaToArray(request.param("index")));
        reloadAnalyzersRequest.indicesOptions(IndicesOptions.fromRequest(request, reloadAnalyzersRequest.indicesOptions()));
        return channel -> client.execute(ReloadAnalyzerAction.INSTANCE, reloadAnalyzersRequest, new RestToXContentListener<>(channel));
    }
}
