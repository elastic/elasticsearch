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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetRecordsAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;

public class RestGetRecordsAction extends BaseRestHandler {

    private final GetRecordsAction.TransportAction transportAction;

    @Inject
    public RestGetRecordsAction(Settings settings, RestController controller, GetRecordsAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(
                RestRequest.Method.GET, PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName() + "}/records", this);
        controller.registerHandler(
                RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "results/{" + Job.ID.getPreferredName() + "}/records", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        final GetRecordsAction.Request request;
        BytesReference bodyBytes = restRequest.content();
        if (bodyBytes != null && bodyBytes.length() > 0) {
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = GetRecordsAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        }
        else {
            request = new GetRecordsAction.Request(jobId);
            request.setStart(restRequest.param(GetRecordsAction.Request.START.getPreferredName()));
            request.setEnd(restRequest.param(GetRecordsAction.Request.END.getPreferredName()));
            request.setIncludeInterim(restRequest.paramAsBoolean(GetRecordsAction.Request.INCLUDE_INTERIM.getPreferredName(), false));
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
            request.setAnomalyScore(
                    Double.parseDouble(restRequest.param(GetRecordsAction.Request.ANOMALY_SCORE_FILTER.getPreferredName(), "0.0")));
            request.setSort(restRequest.param(GetRecordsAction.Request.SORT.getPreferredName(),
                    AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName()));
            request.setDecending(restRequest.paramAsBoolean(GetRecordsAction.Request.DESCENDING.getPreferredName(), true));
            request.setMaxNormalizedProbability(
                    Double.parseDouble(restRequest.param(GetRecordsAction.Request.MAX_NORMALIZED_PROBABILITY.getPreferredName(), "0.0")));
            String partitionValue = restRequest.param(GetRecordsAction.Request.PARTITION_VALUE.getPreferredName());
            if (partitionValue != null) {
                request.setPartitionValue(partitionValue);
            }
        }

        return channel -> transportAction.execute(request, new RestToXContentListener<>(channel));
    }
}
