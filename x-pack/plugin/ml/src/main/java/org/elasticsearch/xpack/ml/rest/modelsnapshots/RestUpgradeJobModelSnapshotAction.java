/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction.Request.DEFAULT_TIMEOUT;

public class RestUpgradeJobModelSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST,
                MachineLearning.BASE_PATH
                    + "anomaly_detectors/{"
                    + Job.ID.getPreferredName()
                    + "}/model_snapshots/{"
                    + UpgradeJobModelSnapshotAction.Request.SNAPSHOT_ID.getPreferredName()
                    + "}/_upgrade")
        );
    }

    @Override
    public String getName() {
        return "ml_upgrade_job_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

        String jobId = restRequest.param(Job.ID.getPreferredName());
        String snapshotId = restRequest.param(UpgradeJobModelSnapshotAction.Request.SNAPSHOT_ID.getPreferredName());
        TimeValue timeout = TimeValue.parseTimeValue(
            restRequest.param(UpgradeJobModelSnapshotAction.Request.TIMEOUT.getPreferredName(), DEFAULT_TIMEOUT.getStringRep()),
            UpgradeJobModelSnapshotAction.Request.TIMEOUT.getPreferredName());
        boolean waitForCompletion = restRequest.paramAsBoolean(UpgradeJobModelSnapshotAction.Request.WAIT_FOR_COMPLETION.getPreferredName(),
            false);
        UpgradeJobModelSnapshotAction.Request request = new UpgradeJobModelSnapshotAction.Request(jobId,
            snapshotId,
            timeout,
            waitForCompletion);
        return channel -> client.execute(UpgradeJobModelSnapshotAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
