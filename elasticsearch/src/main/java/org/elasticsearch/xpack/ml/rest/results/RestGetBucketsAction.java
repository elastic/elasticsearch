/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.PageParams;

import java.io.IOException;

public class RestGetBucketsAction extends BaseRestHandler {

    private final GetBucketsAction.TransportAction transportAction;

    @Inject
    public RestGetBucketsAction(Settings settings, RestController controller, GetBucketsAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(RestRequest.Method.GET,
                MlPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName()
                        + "}/results/buckets/{" + Bucket.TIMESTAMP.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                MlPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName()
                        + "}/results/buckets/{" + Bucket.TIMESTAMP.getPreferredName() + "}", this);

        controller.registerHandler(RestRequest.Method.GET,
                MlPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/buckets", this);
        controller.registerHandler(RestRequest.Method.POST,
                MlPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/buckets", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        final GetBucketsAction.Request request;
        if (restRequest.hasContent()) {
            XContentParser parser = restRequest.contentParser();
            request = GetBucketsAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            request = new GetBucketsAction.Request(jobId);

            // Check if the REST param is set first so mutually exclusive
            // options will only cause an error if set
            if (restRequest.hasParam(GetBucketsAction.Request.TIMESTAMP.getPreferredName())) {
                String timestamp = restRequest.param(GetBucketsAction.Request.TIMESTAMP.getPreferredName());
                if (timestamp != null && !timestamp.isEmpty()) {
                    request.setTimestamp(timestamp);
                }
            }
            // multiple bucket options
            if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
                request.setPageParams(
                        new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                                restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.START.getPreferredName())) {
                request.setStart(restRequest.param(GetBucketsAction.Request.START.getPreferredName()));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.END.getPreferredName())) {
                request.setEnd(restRequest.param(GetBucketsAction.Request.END.getPreferredName()));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.ANOMALY_SCORE.getPreferredName())) {
                request.setAnomalyScore(
                        Double.parseDouble(restRequest.param(GetBucketsAction.Request.ANOMALY_SCORE.getPreferredName(), "0.0")));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.MAX_NORMALIZED_PROBABILITY.getPreferredName())) {
                request.setMaxNormalizedProbability(
                        Double.parseDouble(restRequest.param(
                                GetBucketsAction.Request.MAX_NORMALIZED_PROBABILITY.getPreferredName(), "0.0")));
            }
            if (restRequest.hasParam(GetBucketsAction.Request.PARTITION_VALUE.getPreferredName())) {
                request.setPartitionValue(restRequest.param(GetBucketsAction.Request.PARTITION_VALUE.getPreferredName()));
            }

            // single and multiple bucket options
            request.setExpand(restRequest.paramAsBoolean(GetBucketsAction.Request.EXPAND.getPreferredName(), false));
            request.setIncludeInterim(restRequest.paramAsBoolean(GetBucketsAction.Request.INCLUDE_INTERIM.getPreferredName(), false));
        }

        return channel -> transportAction.execute(request, new RestToXContentListener<>(channel));
    }
}
