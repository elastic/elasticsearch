/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction.Request;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.action.util.PageParams;

import java.io.IOException;

public class RestGetCategoriesAction extends BaseRestHandler {

    public RestGetCategoriesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories", this);

        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categories", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request;
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String categoryId = restRequest.param(Request.CATEGORY_ID.getPreferredName());
        BytesReference bodyBytes = restRequest.content();

        if (bodyBytes != null && bodyBytes.length() > 0) {
            XContentParser parser = restRequest.contentParser();
            request = GetCategoriesAction.Request.parseRequest(jobId, parser);
            request.setCategoryId(categoryId);
        } else {

            request = new Request(jobId);
            if (!Strings.isNullOrEmpty(categoryId)) {
                request.setCategoryId(categoryId);
            }
            if (restRequest.hasParam(Request.FROM.getPreferredName())
                    || restRequest.hasParam(Request.SIZE.getPreferredName())
                    || Strings.isNullOrEmpty(categoryId)){

                request.setPageParams(new PageParams(
                        restRequest.paramAsInt(Request.FROM.getPreferredName(), 0),
                        restRequest.paramAsInt(Request.SIZE.getPreferredName(), 100)
                ));
            }
        }

        return channel -> client.execute(GetCategoriesAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
