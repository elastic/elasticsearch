/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.influencers;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetInfluencersAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;

public class RestGetInfluencersAction extends BaseRestHandler {

    private final GetInfluencersAction.TransportAction transportAction;

    @Inject
    public RestGetInfluencersAction(Settings settings, RestController controller, GetInfluencersAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(RestRequest.Method.GET,
                PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName() + "}/influencers", this);
        // endpoints that support body parameters must also accept POST
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName() + "}/influencers", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String start = restRequest.param(GetInfluencersAction.Request.START.getPreferredName());
        String end = restRequest.param(GetInfluencersAction.Request.END.getPreferredName());
        BytesReference bodyBytes = restRequest.content();
        final GetInfluencersAction.Request request;
        if (bodyBytes != null && bodyBytes.length() > 0) {
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = GetInfluencersAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            request = new GetInfluencersAction.Request(jobId);
            request.setStart(start);
            request.setEnd(end);
            request.setIncludeInterim(restRequest.paramAsBoolean(GetInfluencersAction.Request.INCLUDE_INTERIM.getPreferredName(), false));
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
            request.setAnomalyScore(
                    Double.parseDouble(restRequest.param(GetInfluencersAction.Request.ANOMALY_SCORE.getPreferredName(), "0.0")));
            request.setSort(restRequest.param(GetInfluencersAction.Request.SORT_FIELD.getPreferredName(),
                    Influencer.ANOMALY_SCORE.getPreferredName()));
            request.setDecending(restRequest.paramAsBoolean(GetInfluencersAction.Request.DESCENDING_SORT.getPreferredName(), true));
        }

        return channel -> transportAction.execute(request, new RestToXContentListener<GetInfluencersAction.Response>(channel));
    }
}
