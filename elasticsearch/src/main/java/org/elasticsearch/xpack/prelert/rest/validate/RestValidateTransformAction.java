/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.ValidateTransformAction;

import java.io.IOException;

public class RestValidateTransformAction extends BaseRestHandler {

    private ValidateTransformAction.TransportAction transportValidateAction;

    @Inject
    public RestValidateTransformAction(Settings settings, RestController controller,
            ValidateTransformAction.TransportAction transportValidateAction) {
        super(settings);
        this.transportValidateAction = transportValidateAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "_validate/transform", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        BytesReference bodyBytes = restRequest.contentOrSourceParam();
        XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
        ValidateTransformAction.Request validateDetectorRequest = ValidateTransformAction.Request.parseRequest(parser,
                () -> parseFieldMatcher);
        return channel -> transportValidateAction.execute(validateDetectorRequest,
                new AcknowledgedRestListener<ValidateTransformAction.Response>(channel));
    }

}
