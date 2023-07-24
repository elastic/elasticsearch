/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetBucketsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets/{" + Result.TIMESTAMP + "}")
                .replaces(
                    GET,
                    PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets/{" + Result.TIMESTAMP + "}",
                    RestApiVersion.V_7
                )
                .build(),
            Route.builder(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets/{" + Result.TIMESTAMP + "}")
                .replaces(
                    POST,
                    PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets/{" + Result.TIMESTAMP + "}",
                    RestApiVersion.V_7
                )
                .build(),
            Route.builder(GET, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets")
                .replaces(GET, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets", RestApiVersion.V_7)
                .build(),
            Route.builder(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets")
                .replaces(POST, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/buckets", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_get_buckets_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String timestamp = restRequest.param(GetBucketsAction.Request.TIMESTAMP.getPreferredName());
        final GetBucketsAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = GetBucketsAction.Request.parseRequest(jobId, parser);

            // A timestamp in the URL overrides any timestamp that may also have been set in the body
            if (Strings.isNullOrEmpty(timestamp) == false) {
                request.setTimestamp(timestamp);
            }
        } else {
            request = new GetBucketsAction.Request(jobId);

            // Check if the REST param is set first so mutually exclusive
            // options will cause an error if set
            if (Strings.isNullOrEmpty(timestamp) == false) {
                request.setTimestamp(timestamp);
            }
            // multiple bucket options
            if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
                request.setPageParams(
                    new PageParams(
                        restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                        restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                    )
                );
            }
            if (restRequest.hasParam(GetBucketsAction.Request.START.getPreferredName())) {
                request.setStart(restRequest.param(GetBucketsAction.Request.START.getPreferredName()));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.END.getPreferredName())) {
                request.setEnd(restRequest.param(GetBucketsAction.Request.END.getPreferredName()));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.ANOMALY_SCORE.getPreferredName())) {
                request.setAnomalyScore(
                    Double.parseDouble(restRequest.param(GetBucketsAction.Request.ANOMALY_SCORE.getPreferredName(), "0.0"))
                );
            }
            if (restRequest.hasParam(GetBucketsAction.Request.SORT.getPreferredName())) {
                request.setSort(restRequest.param(GetBucketsAction.Request.SORT.getPreferredName()));
            }
            request.setDescending(
                restRequest.paramAsBoolean(GetBucketsAction.Request.DESCENDING.getPreferredName(), request.isDescending())
            );

            // single and multiple bucket options
            request.setExpand(restRequest.paramAsBoolean(GetBucketsAction.Request.EXPAND.getPreferredName(), false));
            request.setExcludeInterim(restRequest.paramAsBoolean(GetBucketsAction.Request.EXCLUDE_INTERIM.getPreferredName(), false));
        }

        return channel -> client.execute(GetBucketsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
