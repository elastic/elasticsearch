/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.validate;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.ValidateDetectorAction;

import java.io.IOException;

public class RestValidateDetectorAction extends BaseRestHandler {

    private ValidateDetectorAction.TransportAction transportValidateAction;

    @Inject
    public RestValidateDetectorAction(Settings settings, RestController controller,
            ValidateDetectorAction.TransportAction transportValidateAction) {
        super(settings);
        this.transportValidateAction = transportValidateAction;
        controller.registerHandler(RestRequest.Method.POST, MlPlugin.BASE_PATH + "_validate/detector", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentOrSourceParamParser();
        ValidateDetectorAction.Request validateDetectorRequest = ValidateDetectorAction.Request.parseRequest(parser);
        return channel -> transportValidateAction.execute(validateDetectorRequest,
                new AcknowledgedRestListener<ValidateDetectorAction.Response>(channel));
    }

}
