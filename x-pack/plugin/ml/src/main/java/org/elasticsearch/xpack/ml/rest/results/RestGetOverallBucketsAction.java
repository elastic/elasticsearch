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
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestGetOverallBucketsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "anomaly_detectors/{" + ID + "}/results/overall_buckets"),
            new Route(POST, BASE_PATH + "anomaly_detectors/{" + ID + "}/results/overall_buckets")
        );
    }

    @Override
    public String getName() {
        return "ml_get_overall_buckets_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        final Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = Request.parseRequest(jobId, parser);
        } else {
            request = new Request(jobId);
            request.setTopN(restRequest.paramAsInt(Request.TOP_N.getPreferredName(), request.getTopN()));
            if (restRequest.hasParam(Request.BUCKET_SPAN.getPreferredName())) {
                request.setBucketSpan(restRequest.param(Request.BUCKET_SPAN.getPreferredName()));
            }
            request.setOverallScore(Double.parseDouble(restRequest.param(Request.OVERALL_SCORE.getPreferredName(), "0.0")));
            request.setExcludeInterim(restRequest.paramAsBoolean(Request.EXCLUDE_INTERIM.getPreferredName(), request.isExcludeInterim()));
            if (restRequest.hasParam(Request.START.getPreferredName())) {
                request.setStart(restRequest.param(Request.START.getPreferredName()));
            }
            if (restRequest.hasParam(Request.END.getPreferredName())) {
                request.setEnd(restRequest.param(Request.END.getPreferredName()));
            }
            request.setAllowNoMatch(restRequest.paramAsBoolean(Request.ALLOW_NO_MATCH.getPreferredName(), request.allowNoMatch()));
        }

        return channel -> client.execute(GetOverallBucketsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
