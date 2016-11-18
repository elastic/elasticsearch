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
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetJobsAction;
import org.elasticsearch.xpack.prelert.action.GetJobsAction.Response;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;

public class RestGetJobsAction extends BaseRestHandler {
    private static final int DEFAULT_FROM = 0;
    private static final int DEFAULT_SIZE = 100;

    private final GetJobsAction.TransportAction transportGetJobsAction;

    @Inject
    public RestGetJobsAction(Settings settings, RestController controller, GetJobsAction.TransportAction transportGetJobsAction) {
        super(settings);
        this.transportGetJobsAction = transportGetJobsAction;
        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH + "jobs", this);
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
            request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), DEFAULT_SIZE)));
        }
        return channel -> transportGetJobsAction.execute(request, new RestToXContentListener<Response>(channel));
    }
}
