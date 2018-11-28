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
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.ml.featureindexbuilder.FeatureIndexBuilder;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StartFeatureIndexBuilderJobAction;

import java.io.IOException;

public class RestStartFeatureIndexBuilderJobAction extends BaseRestHandler {
    
    public RestStartFeatureIndexBuilderJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, FeatureIndexBuilder.BASE_PATH_JOBS_BY_ID + "_start", this);
    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(RollupField.ID.getPreferredName());
        StartFeatureIndexBuilderJobAction.Request request = new StartFeatureIndexBuilderJobAction.Request(id);

        return channel -> client.execute(StartFeatureIndexBuilderJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "ml_feature_index_builder_start_job_action";
    }
}
