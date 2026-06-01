/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig.ID;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDatafeedsAction extends BaseRestHandler {

    private final boolean cpsAuthVisibilityEnabled;

    public RestGetDatafeedsAction(Settings settings) {
        this.cpsAuthVisibilityEnabled = DatafeedManager.crossProjectMlEnabled(settings);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH + "datafeeds/{" + ID + "}"), new Route(GET, BASE_PATH + "datafeeds"));
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
        request.setAllowNoMatch(restRequest.paramAsBoolean(GetDatafeedsStatsAction.Request.ALLOW_NO_MATCH, request.allowNoMatch()));
        final ToXContent.Params xContentParams = cpsAuthVisibilityEnabled
            ? new ToXContent.DelegatingMapParams(Map.of(DatafeedConfig.CPS_AUTH_VISIBILITY_PARAM, "true"), restRequest)
            : restRequest;
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetDatafeedsAction.INSTANCE,
            request,
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetDatafeedsAction.Response response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, xContentParams);
                    return new RestResponse(RestStatus.OK, builder);
                }
            }
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(EXCLUDE_GENERATED);
    }
}
