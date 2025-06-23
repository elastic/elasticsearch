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
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetRecordsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "anomaly_detectors/{" + ID + "}/results/records"),
            new Route(POST, BASE_PATH + "anomaly_detectors/{" + ID + "}/results/records")
        );
    }

    @Override
    public String getName() {
        return "ml_get_records_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        final GetRecordsAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = GetRecordsAction.Request.parseRequest(jobId, parser);
        } else {
            request = new GetRecordsAction.Request(jobId);
            request.setStart(restRequest.param(GetRecordsAction.Request.START.getPreferredName()));
            request.setEnd(restRequest.param(GetRecordsAction.Request.END.getPreferredName()));
            request.setExcludeInterim(
                restRequest.paramAsBoolean(GetRecordsAction.Request.EXCLUDE_INTERIM.getPreferredName(), request.isExcludeInterim())
            );
            request.setPageParams(
                new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                )
            );
            request.setRecordScore(
                Double.parseDouble(
                    restRequest.param(
                        GetRecordsAction.Request.RECORD_SCORE_FILTER.getPreferredName(),
                        String.valueOf(request.getRecordScoreFilter())
                    )
                )
            );
            request.setSort(restRequest.param(GetRecordsAction.Request.SORT.getPreferredName(), request.getSort()));
            request.setDescending(
                restRequest.paramAsBoolean(GetRecordsAction.Request.DESCENDING.getPreferredName(), request.isDescending())
            );
        }

        return channel -> client.execute(GetRecordsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
