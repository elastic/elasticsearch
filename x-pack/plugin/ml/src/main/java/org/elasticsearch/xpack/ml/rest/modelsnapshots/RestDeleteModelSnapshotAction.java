/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteModelSnapshotAction extends BaseRestHandler {

   @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(DELETE, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() +
                "}/model_snapshots/{" + ModelSnapshotField.SNAPSHOT_ID.getPreferredName() + "}")
        );
    }

    @Override
    public String getName() {
        return "ml_delete_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteModelSnapshotAction.Request deleteModelSnapshot = new DeleteModelSnapshotAction.Request(
                restRequest.param(Job.ID.getPreferredName()),
                restRequest.param(ModelSnapshotField.SNAPSHOT_ID.getPreferredName()));

        return channel -> client.execute(DeleteModelSnapshotAction.INSTANCE, deleteModelSnapshot, new RestToXContentListener<>(channel));
    }
}
