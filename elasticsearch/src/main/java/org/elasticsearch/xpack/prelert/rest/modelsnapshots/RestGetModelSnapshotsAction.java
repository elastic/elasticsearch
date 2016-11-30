/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.modelsnapshots;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
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
import org.elasticsearch.xpack.prelert.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;

public class RestGetModelSnapshotsAction extends BaseRestHandler {

    private static final ParseField JOB_ID = new ParseField("jobId");
    private static final ParseField SORT = new ParseField("sort");
    private static final ParseField DESC_ORDER = new ParseField("desc");
    private static final ParseField SIZE = new ParseField("size");
    private static final ParseField FROM = new ParseField("from");
    private static final ParseField START = new ParseField("start");
    private static final ParseField END = new ParseField("end");
    private static final ParseField DESCRIPTION = new ParseField("description");

    // Even though these are null, setting up the defaults in case
    // we want to change them later
    private final String DEFAULT_SORT = null;
    private final String DEFAULT_START = null;
    private final String DEFAULT_END = null;
    private final String DEFAULT_DESCRIPTION = null;
    private final boolean DEFAULT_DESC_ORDER = true;

    private final GetModelSnapshotsAction.TransportAction transportGetModelSnapshotsAction;

    @Inject
    public RestGetModelSnapshotsAction(Settings settings, RestController controller,
            GetModelSnapshotsAction.TransportAction transportGetModelSnapshotsAction) {
        super(settings);
        this.transportGetModelSnapshotsAction = transportGetModelSnapshotsAction;
        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH + "modelsnapshots/{jobId}", this);
        // endpoints that support body parameters must also accept POST
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "modelsnapshots/{jobId}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(JOB_ID.getPreferredName());
        GetModelSnapshotsAction.Request getModelSnapshots;
        if (RestActions.hasBodyContent(restRequest)) {
            BytesReference bodyBytes = RestActions.getRestContent(restRequest);
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            getModelSnapshots = GetModelSnapshotsAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            getModelSnapshots = new GetModelSnapshotsAction.Request(jobId);
            getModelSnapshots.setSort(restRequest.param(SORT.getPreferredName(), DEFAULT_SORT));
            if (restRequest.hasParam(START.getPreferredName())) {
                getModelSnapshots.setStart(restRequest.param(START.getPreferredName(), DEFAULT_START));
            }
            if (restRequest.hasParam(END.getPreferredName())) {
                getModelSnapshots.setEnd(restRequest.param(END.getPreferredName(), DEFAULT_END));
            }
            if (restRequest.hasParam(DESCRIPTION.getPreferredName())) {
                getModelSnapshots.setDescriptionString(restRequest.param(DESCRIPTION.getPreferredName(), DEFAULT_DESCRIPTION));
            }
            getModelSnapshots.setDescOrder(restRequest.paramAsBoolean(DESC_ORDER.getPreferredName(), DEFAULT_DESC_ORDER));
            getModelSnapshots.setPageParams(new PageParams(restRequest.paramAsInt(FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }

        return channel -> transportGetModelSnapshotsAction.execute(getModelSnapshots, new RestToXContentListener<>(channel));
    }
}
