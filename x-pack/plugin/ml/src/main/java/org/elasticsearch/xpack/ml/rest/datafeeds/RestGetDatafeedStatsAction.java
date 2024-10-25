/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.DEPRECATED_ALLOW_NO_DATAFEEDS_PARAM;
import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.rest.RestCompatibilityChecker.checkAndSetDeprecatedParam;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDatafeedStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH + "datafeeds/{" + ID + "}/_stats"), new Route(GET, BASE_PATH + "datafeeds/_stats"));
    }

    @Override
    public String getName() {
        return "ml_get_datafeed_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        if (Strings.isNullOrEmpty(datafeedId)) {
            datafeedId = GetDatafeedsStatsAction.ALL;
        }
        @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING) // v7 REST API no longer exists: eliminate ref to RestApiVersion.V_7
        Request request = new Request(datafeedId);
        checkAndSetDeprecatedParam(
            DEPRECATED_ALLOW_NO_DATAFEEDS_PARAM,
            Request.ALLOW_NO_MATCH,
            RestApiVersion.V_7,
            restRequest,
            (r, s) -> r.paramAsBoolean(s, request.allowNoMatch()),
            request::setAllowNoMatch
        );
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetDatafeedsStatsAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }
}
