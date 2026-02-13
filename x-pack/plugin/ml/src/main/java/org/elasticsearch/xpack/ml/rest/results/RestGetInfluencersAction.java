/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetInfluencersAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "anomaly_detectors/{" + ID + "}/results/influencers"),
            new Route(POST, BASE_PATH + "anomaly_detectors/{" + ID + "}/results/influencers")
        );
    }

    @Override
    public String getName() {
        return "ml_get_influencers_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String start = restRequest.param(GetInfluencersAction.Request.START.getPreferredName());
        String end = restRequest.param(GetInfluencersAction.Request.END.getPreferredName());
        final GetInfluencersAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = GetInfluencersAction.Request.parseRequest(jobId, parser);
        } else {
            request = new GetInfluencersAction.Request(jobId);
            request.setStart(start);
            request.setEnd(end);
            request.setExcludeInterim(
                restRequest.paramAsBoolean(GetInfluencersAction.Request.EXCLUDE_INTERIM.getPreferredName(), request.isExcludeInterim())
            );
            request.setPageParams(
                new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                )
            );
            request.setInfluencerScore(
                Double.parseDouble(
                    restRequest.param(
                        GetInfluencersAction.Request.INFLUENCER_SCORE.getPreferredName(),
                        String.valueOf(request.getInfluencerScore())
                    )
                )
            );
            request.setSort(restRequest.param(GetInfluencersAction.Request.SORT_FIELD.getPreferredName(), request.getSort()));
            request.setDescending(
                restRequest.paramAsBoolean(GetInfluencersAction.Request.DESCENDING_SORT.getPreferredName(), request.isDescending())
            );
        }

        return channel -> client.execute(GetInfluencersAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
