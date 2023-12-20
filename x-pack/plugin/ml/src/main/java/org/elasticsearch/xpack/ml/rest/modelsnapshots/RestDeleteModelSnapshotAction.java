/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField.SNAPSHOT_ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestDeleteModelSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/model_snapshots/{" + SNAPSHOT_ID + "}")
                .replaces(
                    DELETE,
                    PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/model_snapshots/{" + SNAPSHOT_ID + "}",
                    RestApiVersion.V_7
                )
                .build()
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
            restRequest.param(SNAPSHOT_ID.getPreferredName())
        );

        return channel -> client.execute(DeleteModelSnapshotAction.INSTANCE, deleteModelSnapshot, new RestToXContentListener<>(channel));
    }
}
