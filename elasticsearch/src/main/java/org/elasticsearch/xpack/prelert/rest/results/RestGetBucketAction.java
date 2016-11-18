/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.results;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetBucketAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;

public class RestGetBucketAction extends BaseRestHandler {

    private final GetBucketAction.TransportAction transportAction;

    @Inject
    public RestGetBucketAction(Settings settings, RestController controller, GetBucketAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(RestRequest.Method.GET,
                PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName()
                        + "}/bucket/{" + Bucket.TIMESTAMP.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName()
                        + "}/bucket/{" + Bucket.TIMESTAMP.getPreferredName() + "}", this);

        controller.registerHandler(RestRequest.Method.GET,
                PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName() + "}/bucket", this);
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName() + "}/bucket", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        BytesReference bodyBytes = restRequest.content();
        final GetBucketAction.Request request;
        if (bodyBytes != null && bodyBytes.length() > 0) {
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = GetBucketAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            request = new GetBucketAction.Request(jobId);
            String timestamp = restRequest.param(GetBucketAction.Request.TIMESTAMP.getPreferredName());
            String start = restRequest.param(GetBucketAction.Request.START.getPreferredName());
            String end = restRequest.param(GetBucketAction.Request.END.getPreferredName());

            // Single bucket
            if (timestamp != null && !timestamp.isEmpty()) {
                request.setTimestamp(timestamp);
                request.setExpand(restRequest.paramAsBoolean(GetBucketAction.Request.EXPAND.getPreferredName(), false));
                request.setIncludeInterim(restRequest.paramAsBoolean(GetBucketAction.Request.INCLUDE_INTERIM.getPreferredName(), false));
            } else if (start != null && !start.isEmpty() && end != null && !end.isEmpty()) {
                // Multiple buckets
                request.setStart(start);
                request.setEnd(end);
                request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), 0),
                        restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), 100)));
                request.setAnomalyScore(
                        Double.parseDouble(restRequest.param(GetBucketAction.Request.ANOMALY_SCORE.getPreferredName(), "0.0")));
                request.setMaxNormalizedProbability(
                        Double.parseDouble(restRequest.param(
                                GetBucketAction.Request.MAX_NORMALIZED_PROBABILITY.getPreferredName(), "0.0")));
                if (restRequest.hasParam(GetBucketAction.Request.PARTITION_VALUE.getPreferredName())) {
                    request.setPartitionValue(restRequest.param(GetBucketAction.Request.PARTITION_VALUE.getPreferredName()));
                }
            } else {
                throw new IllegalArgumentException("Either [timestamp] or [start, end] parameters must be set.");
            }

            // Common options
            request.setExpand(restRequest.paramAsBoolean(GetBucketAction.Request.EXPAND.getPreferredName(), false));
            request.setIncludeInterim(restRequest.paramAsBoolean(GetBucketAction.Request.INCLUDE_INTERIM.getPreferredName(), false));
        }

        return channel -> transportAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}
