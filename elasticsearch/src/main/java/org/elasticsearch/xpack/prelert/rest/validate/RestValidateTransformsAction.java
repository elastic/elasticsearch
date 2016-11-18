/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.validate;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.ValidateTransformsAction;

import java.io.IOException;

public class RestValidateTransformsAction extends BaseRestHandler {

    private ValidateTransformsAction.TransportAction transportValidateAction;

    @Inject
    public RestValidateTransformsAction(Settings settings, RestController controller,
            ValidateTransformsAction.TransportAction transportValidateAction) {
        super(settings);
        this.transportValidateAction = transportValidateAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "_validate/transforms", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        BytesReference bodyBytes = RestActions.getRestContent(restRequest);
        XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
        ValidateTransformsAction.Request validateDetectorRequest = ValidateTransformsAction.Request.PARSER.apply(parser,
                () -> parseFieldMatcher);
        return channel -> transportValidateAction.execute(validateDetectorRequest,
                new AcknowledgedRestListener<ValidateTransformsAction.Response>(channel));
    }

}
