/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ccr.action.CcrStatsAction;

import java.io.IOException;

public class RestCcrStatsAction extends BaseRestHandler {

    public RestCcrStatsAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/_ccr/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/_ccr/stats/{index}", this);
    }

    @Override
    public String getName() {
        return "ccr_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final CcrStatsAction.TasksRequest request = new CcrStatsAction.TasksRequest();
        request.setIndices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        request.setIndicesOptions(IndicesOptions.fromRequest(restRequest, request.indicesOptions()));
        return channel -> client.execute(CcrStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
