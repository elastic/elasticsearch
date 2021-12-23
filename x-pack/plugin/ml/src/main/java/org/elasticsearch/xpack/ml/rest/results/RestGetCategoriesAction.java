/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.action.GetCategoriesAction.Request.CATEGORY_ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

public class RestGetCategoriesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories/{" + CATEGORY_ID + "}")
                .replaces(
                    GET,
                    PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories/{" + CATEGORY_ID + "}",
                    RestApiVersion.V_7
                )
                .build(),
            Route.builder(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories/{" + CATEGORY_ID + "}")
                .replaces(
                    POST,
                    PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories/{" + CATEGORY_ID + "}",
                    RestApiVersion.V_7
                )
                .build(),
            Route.builder(GET, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories")
                .replaces(GET, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories", RestApiVersion.V_7)
                .build(),
            Route.builder(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories")
                .replaces(POST, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/results/categories", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_get_categories_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request;
        String jobId = restRequest.param(Job.ID.getPreferredName());
        Long categoryId = restRequest.hasParam(CATEGORY_ID.getPreferredName())
            ? Long.parseLong(restRequest.param(CATEGORY_ID.getPreferredName()))
            : null;

        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = GetCategoriesAction.Request.parseRequest(jobId, parser);
            if (categoryId != null) {
                request.setCategoryId(categoryId);
            }
        } else {
            request = new Request(jobId);
            if (categoryId != null) {
                request.setCategoryId(categoryId);
            }
            if (restRequest.hasParam(Request.FROM.getPreferredName())
                || restRequest.hasParam(Request.SIZE.getPreferredName())
                || categoryId == null) {

                request.setPageParams(
                    new PageParams(
                        restRequest.paramAsInt(Request.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                        restRequest.paramAsInt(Request.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                    )
                );
            }
            request.setPartitionFieldValue(restRequest.param(Request.PARTITION_FIELD_VALUE.getPreferredName()));
        }

        return channel -> client.execute(GetCategoriesAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
