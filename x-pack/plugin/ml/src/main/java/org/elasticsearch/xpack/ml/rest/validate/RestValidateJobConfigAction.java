/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.validate;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestValidateJobConfigAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "anomaly_detectors/_validate"));
    }

    @Override
    public String getName() {
        return "ml_validate_job_config_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentOrSourceParamParser();
        ValidateJobConfigAction.Request validateConfigRequest = ValidateJobConfigAction.Request.parseRequest(parser);
        return channel -> client.execute(ValidateJobConfigAction.INSTANCE, validateConfigRequest, new RestToXContentListener<>(channel));
    }

}
