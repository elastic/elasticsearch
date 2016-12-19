/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.action.OpenJobAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestOpenJobAction extends BaseRestHandler {

    private final OpenJobAction.TransportAction openJobAction;

    @Inject
    public RestOpenJobAction(Settings settings, RestController controller, OpenJobAction.TransportAction openJobAction) {
        super(settings);
        this.openJobAction = openJobAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_open", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        OpenJobAction.Request request = new OpenJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        request.setIgnoreDowntime(restRequest.paramAsBoolean(JobDataAction.Request.IGNORE_DOWNTIME.getPreferredName(), false));
        if (restRequest.hasParam(OpenJobAction.Request.OPEN_TIMEOUT.getPreferredName())) {
            request.setOpenTimeout(TimeValue.parseTimeValue(
                    restRequest.param(OpenJobAction.Request.OPEN_TIMEOUT.getPreferredName()),
                    OpenJobAction.Request.OPEN_TIMEOUT.getPreferredName()));
        }
        return channel -> openJobAction.execute(request, new AcknowledgedRestListener<>(channel));
    }
}
