/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction.Request.SNAPSHOT_ID;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestRevertModelSnapshotAction extends BaseRestHandler {

    private static final boolean DELETE_INTERVENING_DEFAULT = false;

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "anomaly_detectors/{" + ID + "}/model_snapshots/{" + SNAPSHOT_ID + "}/_revert"));
    }

    @Override
    public String getName() {
        return "ml_revert_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String snapshotId = restRequest.param(SNAPSHOT_ID.getPreferredName());
        RevertModelSnapshotAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = RevertModelSnapshotAction.Request.parseRequest(jobId, snapshotId, parser);
        } else {
            request = new RevertModelSnapshotAction.Request(jobId, snapshotId);
            request.setDeleteInterveningResults(
                restRequest.paramAsBoolean(
                    RevertModelSnapshotAction.Request.DELETE_INTERVENING.getPreferredName(),
                    DELETE_INTERVENING_DEFAULT
                )
            );
        }
        request.ackTimeout(getAckTimeout(restRequest));
        request.masterNodeTimeout(getMasterNodeTimeout(restRequest));

        return channel -> client.execute(RevertModelSnapshotAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
