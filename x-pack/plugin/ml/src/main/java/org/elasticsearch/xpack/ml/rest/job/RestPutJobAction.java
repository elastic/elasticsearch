/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPutJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(PUT, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}")
                .replaces(PUT, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_put_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(restRequest, SearchRequest.DEFAULT_INDICES_OPTIONS);
        PutJobAction.Request putJobRequest = PutJobAction.Request.parseRequest(jobId, parser, indicesOptions);
        putJobRequest.timeout(restRequest.paramAsTime("timeout", putJobRequest.timeout()));
        putJobRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", putJobRequest.masterNodeTimeout()));

        return channel -> client.execute(PutJobAction.INSTANCE, putJobRequest, new RestToXContentListener<>(channel));
    }

}
