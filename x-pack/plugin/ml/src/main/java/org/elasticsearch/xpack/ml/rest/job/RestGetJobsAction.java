/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.DEPRECATED_ALLOW_NO_JOBS_PARAM;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;
import static org.elasticsearch.xpack.ml.rest.RestCompatibilityChecker.checkAndSetDeprecatedParam;

@ServerlessScope(Scope.PUBLIC)
public class RestGetJobsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}")
                .replaces(GET, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}", RestApiVersion.V_7)
                .build(),
            Route.builder(GET, BASE_PATH + "anomaly_detectors")
                .replaces(GET, PRE_V7_BASE_PATH + "anomaly_detectors", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_get_jobs_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        if (Strings.isNullOrEmpty(jobId)) {
            jobId = Metadata.ALL;
        }
        Request request = new Request(jobId);
        checkAndSetDeprecatedParam(
            DEPRECATED_ALLOW_NO_JOBS_PARAM,
            Request.ALLOW_NO_MATCH,
            RestApiVersion.V_7,
            restRequest,
            (r, s) -> r.paramAsBoolean(s, request.allowNoMatch()),
            request::setAllowNoMatch
        );
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetJobsAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(EXCLUDE_GENERATED);
    }
}
