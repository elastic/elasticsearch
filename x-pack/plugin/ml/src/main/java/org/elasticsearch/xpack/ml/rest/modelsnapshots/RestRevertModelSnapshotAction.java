/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRevertModelSnapshotAction extends BaseRestHandler {

    private static final boolean DELETE_INTERVENING_DEFAULT = false;

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestRevertModelSnapshotAction.class));

    public RestRevertModelSnapshotAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/model_snapshots/{" +
                RevertModelSnapshotAction.Request.SNAPSHOT_ID.getPreferredName() + "}/_revert", this,
            POST, MachineLearning.PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/model_snapshots/{" +
                RevertModelSnapshotAction.Request.SNAPSHOT_ID.getPreferredName() + "}/_revert", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_revert_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String snapshotId = restRequest.param(RevertModelSnapshotAction.Request.SNAPSHOT_ID.getPreferredName());
        RevertModelSnapshotAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = RevertModelSnapshotAction.Request.parseRequest(jobId, snapshotId, parser);
        } else {
            request = new RevertModelSnapshotAction.Request(jobId, snapshotId);
            request.setDeleteInterveningResults(restRequest
                    .paramAsBoolean(RevertModelSnapshotAction.Request.DELETE_INTERVENING.getPreferredName(), DELETE_INTERVENING_DEFAULT));
        }
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));

        return channel -> client.execute(RevertModelSnapshotAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }
}
