/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import java.io.IOException;

public class RestDeleteListAction extends BaseRestHandler {

    private final DeleteListAction.TransportAction transportAction;

    @Inject
    public RestDeleteListAction(Settings settings, RestController controller, DeleteListAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(RestRequest.Method.DELETE,
                PrelertPlugin.BASE_PATH + "lists/{" + Request.LIST_ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = new Request(restRequest.param(Request.LIST_ID.getPreferredName()));
        return channel -> transportAction.execute(request, new AcknowledgedRestListener<>(channel));
    }

}
