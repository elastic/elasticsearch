/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction.Request;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestGetCategoriesAction extends BaseRestHandler {

    public RestGetCategoriesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories", this);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories", this);

        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_get_catagories_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request;
        String jobId = restRequest.param(Job.ID.getPreferredName());
        Long categoryId = restRequest.hasParam(Request.CATEGORY_ID.getPreferredName()) ? Long.parseLong(
                restRequest.param(Request.CATEGORY_ID.getPreferredName())) : null;

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
                    || categoryId == null){

                request.setPageParams(new PageParams(
                        restRequest.paramAsInt(Request.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                        restRequest.paramAsInt(Request.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                ));
            }
        }

        return channel -> client.execute(GetCategoriesAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
