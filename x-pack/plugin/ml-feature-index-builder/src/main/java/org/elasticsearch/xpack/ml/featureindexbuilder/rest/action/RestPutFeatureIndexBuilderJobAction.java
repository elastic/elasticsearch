/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.featureindexbuilder.FeatureIndexBuilder;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJob;

import java.io.IOException;

public class RestPutFeatureIndexBuilderJobAction extends BaseRestHandler {
    
    public RestPutFeatureIndexBuilderJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, FeatureIndexBuilder.BASE_PATH_JOBS_BY_ID, this);
    }

    @Override
    public String getName() {
        return "feature_index_builder_put_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(FeatureIndexBuilderJob.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();

        PutFeatureIndexBuilderJobAction.Request request = PutFeatureIndexBuilderJobAction.Request.fromXContent(parser, id);

        return channel -> client.execute(PutFeatureIndexBuilderJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
