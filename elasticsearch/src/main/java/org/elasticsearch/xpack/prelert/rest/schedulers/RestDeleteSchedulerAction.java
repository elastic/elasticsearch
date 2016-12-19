/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import java.io.IOException;

public class RestDeleteSchedulerAction extends BaseRestHandler {

    private final DeleteSchedulerAction.TransportAction transportDeleteSchedulerAction;

    @Inject
    public RestDeleteSchedulerAction(Settings settings, RestController controller,
                                     DeleteSchedulerAction.TransportAction transportDeleteSchedulerAction) {
        super(settings);
        this.transportDeleteSchedulerAction = transportDeleteSchedulerAction;
        controller.registerHandler(RestRequest.Method.DELETE, PrelertPlugin.BASE_PATH + "schedulers/{"
                + SchedulerConfig.ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String schedulerId = restRequest.param(SchedulerConfig.ID.getPreferredName());
        DeleteSchedulerAction.Request deleteSchedulerRequest = new DeleteSchedulerAction.Request(schedulerId);
        return channel -> transportDeleteSchedulerAction.execute(deleteSchedulerRequest, new AcknowledgedRestListener<>(channel));
    }

}
