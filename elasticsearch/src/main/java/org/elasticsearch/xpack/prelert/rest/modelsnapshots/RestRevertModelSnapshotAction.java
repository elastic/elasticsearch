/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.modelsnapshots;

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
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestRevertModelSnapshotAction extends BaseRestHandler {

    private final RevertModelSnapshotAction.TransportAction transportAction;

    private final String TIME_DEFAULT = null;
    private final String SNAPSHOT_ID_DEFAULT = null;
    private final String DESCRIPTION_DEFAULT = null;
    private final boolean DELETE_INTERVENING_DEFAULT = false;

    @Inject
    public RestRevertModelSnapshotAction(Settings settings, RestController controller,
            RevertModelSnapshotAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "modelsnapshots/{" + Job.ID.getPreferredName() + "}/_revert",
                this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        RevertModelSnapshotAction.Request request;
        if (RestActions.hasBodyContent(restRequest)) {
            BytesReference bodyBytes = RestActions.getRestContent(restRequest);
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = RevertModelSnapshotAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            request = new RevertModelSnapshotAction.Request(jobId);
            request.setTime(restRequest.param(RevertModelSnapshotAction.Request.TIME.getPreferredName(), TIME_DEFAULT));
            request.setSnapshotId(restRequest.param(RevertModelSnapshotAction.Request.SNAPSHOT_ID.getPreferredName(), SNAPSHOT_ID_DEFAULT));
            request.setDescription(
                    restRequest.param(RevertModelSnapshotAction.Request.DESCRIPTION.getPreferredName(), DESCRIPTION_DEFAULT));
            request.setDeleteInterveningResults(restRequest
                    .paramAsBoolean(RevertModelSnapshotAction.Request.DELETE_INTERVENING.getPreferredName(), DELETE_INTERVENING_DEFAULT));
        }
        return channel -> transportAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}
