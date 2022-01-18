/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.DEPRECATED_ALLOW_NO_DATAFEEDS_PARAM;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;
import static org.elasticsearch.xpack.ml.rest.RestCompatibilityChecker.checkAndSetDeprecatedParam;

public class RestGetDatafeedsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}")
                .replaces(GET, PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}", RestApiVersion.V_7)
                .build(),
            Route.builder(GET, BASE_PATH + "datafeeds").replaces(GET, PRE_V7_BASE_PATH + "datafeeds", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "ml_get_datafeeds_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        if (datafeedId == null) {
            datafeedId = GetDatafeedsAction.ALL;
        }
        Request request = new Request(datafeedId);
        checkAndSetDeprecatedParam(
            DEPRECATED_ALLOW_NO_DATAFEEDS_PARAM,
            GetDatafeedsStatsAction.Request.ALLOW_NO_MATCH,
            RestApiVersion.V_7,
            restRequest,
            (r, s) -> r.paramAsBoolean(s, request.allowNoMatch()),
            request::setAllowNoMatch
        );
        return channel -> client.execute(GetDatafeedsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(EXCLUDE_GENERATED);
    }
}
