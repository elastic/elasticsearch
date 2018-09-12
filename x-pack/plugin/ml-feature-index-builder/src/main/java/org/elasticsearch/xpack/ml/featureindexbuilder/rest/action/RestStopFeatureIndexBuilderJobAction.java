/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.featureindexbuilder.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.featureindexbuilder.FeatureIndexBuilder;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StopFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJob;

import java.io.IOException;

public class RestStopFeatureIndexBuilderJobAction extends BaseRestHandler {

    public RestStopFeatureIndexBuilderJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, FeatureIndexBuilder.BASE_PATH +  "job/{id}/_stop", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(FeatureIndexBuilderJob.ID.getPreferredName());
        StopFeatureIndexBuilderJobAction.Request request = new StopFeatureIndexBuilderJobAction.Request(id);

        return channel -> client.execute(StopFeatureIndexBuilderJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "feature_index_builder_stop_job_action";
    }
}
