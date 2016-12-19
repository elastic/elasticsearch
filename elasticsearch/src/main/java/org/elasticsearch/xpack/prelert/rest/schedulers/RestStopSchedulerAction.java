/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfig;

import java.io.IOException;

public class RestStopSchedulerAction extends BaseRestHandler {

    private final StopSchedulerAction.TransportAction transportJobSchedulerAction;

    @Inject
    public RestStopSchedulerAction(Settings settings, RestController controller,
                                   StopSchedulerAction.TransportAction transportJobSchedulerAction) {
        super(settings);
        this.transportJobSchedulerAction = transportJobSchedulerAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "schedulers/{"
                + SchedulerConfig.ID.getPreferredName() + "}/_stop", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        StopSchedulerAction.Request jobSchedulerRequest = new StopSchedulerAction.Request(
                restRequest.param(SchedulerConfig.ID.getPreferredName()));
        if (restRequest.hasParam("stop_timeout")) {
            jobSchedulerRequest.setStopTimeout(TimeValue.parseTimeValue(restRequest.param("stop_timeout"), "stop_timeout"));
        }
        return channel -> transportJobSchedulerAction.execute(jobSchedulerRequest, new AcknowledgedRestListener<>(channel));
    }
}
