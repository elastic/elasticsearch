/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.validate;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestValidateJobConfigAction extends BaseRestHandler {

    public RestValidateJobConfigAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "anomaly_detectors/_validate", this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.V7_BASE_PATH + "anomaly_detectors/_validate", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_validate_job_config_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentOrSourceParamParser();
        ValidateJobConfigAction.Request validateConfigRequest = ValidateJobConfigAction.Request.parseRequest(parser);
        return channel ->
                client.execute(ValidateJobConfigAction.INSTANCE, validateConfigRequest, new RestToXContentListener<>(channel));
    }

}
