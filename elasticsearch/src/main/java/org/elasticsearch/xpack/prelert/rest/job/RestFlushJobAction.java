/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.job;

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
import org.elasticsearch.xpack.prelert.action.FlushJobAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestFlushJobAction extends BaseRestHandler {

    private final boolean DEFAULT_CALC_INTERIM = false;
    private final String DEFAULT_START = "";
    private final String DEFAULT_END = "";
    private final String DEFAULT_ADVANCE_TIME = "";

    private final FlushJobAction.TransportAction flushJobAction;

    @Inject
    public RestFlushJobAction(Settings settings, RestController controller,
                              FlushJobAction.TransportAction flushJobAction) {
        super(settings);
        this.flushJobAction = flushJobAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}/_flush",
                this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        BytesReference bodyBytes = RestActions.getRestContent(restRequest);
        final FlushJobAction.Request request;
        if (RestActions.hasBodyContent(restRequest)) {
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = FlushJobAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            request = new FlushJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
            request.setCalcInterim(restRequest.paramAsBoolean(FlushJobAction.Request.CALC_INTERIM.getPreferredName(),
                    DEFAULT_CALC_INTERIM));
            request.setStart(restRequest.param(FlushJobAction.Request.START.getPreferredName(), DEFAULT_START));
            request.setEnd(restRequest.param(FlushJobAction.Request.END.getPreferredName(), DEFAULT_END));
            request.setAdvanceTime(restRequest.param(FlushJobAction.Request.ADVANCE_TIME.getPreferredName(), DEFAULT_ADVANCE_TIME));
        }

        return channel -> flushJobAction.execute(request, new AcknowledgedRestListener<>(channel));
    }
}
