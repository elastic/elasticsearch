/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction.Request.SNAPSHOT_ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetJobModelSnapshotsUpgradeStatsAction extends BaseRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/model_snapshots/{" + SNAPSHOT_ID + "}/_upgrade/_stats")
        );
    }

    @Override
    public String getName() {
        return "ml_get_job_model_snapshot_upgrade_stats_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String snapshotId = restRequest.param(SNAPSHOT_ID.getPreferredName());
        GetJobModelSnapshotsUpgradeStatsAction.Request request = new GetJobModelSnapshotsUpgradeStatsAction.Request(jobId, snapshotId);
        request.setAllowNoMatch(
            restRequest.paramAsBoolean(GetJobModelSnapshotsUpgradeStatsAction.Request.ALLOW_NO_MATCH, request.allowNoMatch())
        );
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetJobModelSnapshotsUpgradeStatsAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }
}
