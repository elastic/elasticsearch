/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.PutListAction;

import java.io.IOException;

public class RestPutListAction extends BaseRestHandler {

    private final PutListAction.TransportAction transportCreateListAction;

    @Inject
    public RestPutListAction(Settings settings, RestController controller, PutListAction.TransportAction transportCreateListAction) {
        super(settings);
        this.transportCreateListAction = transportCreateListAction;
        controller.registerHandler(RestRequest.Method.PUT, PrelertPlugin.BASE_PATH + "lists", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        BytesReference bodyBytes = restRequest.contentOrSourceParam();
        XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
        PutListAction.Request putListRequest = PutListAction.Request.parseRequest(parser, () -> parseFieldMatcher);
        return channel -> transportCreateListAction.execute(putListRequest, new AcknowledgedRestListener<>(channel));
    }

}
