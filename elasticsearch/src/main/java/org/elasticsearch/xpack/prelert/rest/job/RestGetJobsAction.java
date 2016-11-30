/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetJobsAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;
import java.util.Set;

public class RestGetJobsAction extends BaseRestHandler {

    private final GetJobsAction.TransportAction transportGetJobAction;

    @Inject
    public RestGetJobsAction(Settings settings, RestController controller, GetJobsAction.TransportAction transportGetJobAction) {
        super(settings);
        this.transportGetJobAction = transportGetJobAction;

        // GETs
        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET,
                PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}/{metric}", this);
        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH + "jobs", this);

        // POSTs
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}/{metric}", this);
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "jobs", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final GetJobsAction.Request request;
        if (RestActions.hasBodyContent(restRequest)) {
            BytesReference bodyBytes = RestActions.getRestContent(restRequest);
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = GetJobsAction.Request.PARSER.apply(parser, () -> parseFieldMatcher);
        } else {
            request = new GetJobsAction.Request();
            request.setJobId(restRequest.param(Job.ID.getPreferredName()));
            Set<String> stats = Strings.splitStringByCommaToSet(
                    restRequest.param(GetJobsAction.Request.METRIC.getPreferredName(), "config"));
            request.setStats(stats);
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }

        return channel -> transportGetJobAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}
