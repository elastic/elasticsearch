/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import java.io.IOException;

public class RestDeleteJobAction extends BaseRestHandler {

    private final DeleteJobAction.TransportAction transportDeleteJobAction;

    @Inject
    public RestDeleteJobAction(Settings settings, RestController controller, DeleteJobAction.TransportAction transportDeleteJobAction) {
        super(settings);
        this.transportDeleteJobAction = transportDeleteJobAction;
        controller.registerHandler(RestRequest.Method.DELETE, PrelertPlugin.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        return channel -> transportDeleteJobAction.execute(deleteJobRequest, new AcknowledgedRestListener<>(channel));
    }
}
