/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;

import java.io.IOException;

public class RestUpdateModelSnapshotAction extends BaseRestHandler {

    public RestUpdateModelSnapshotAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots/{" + ModelSnapshotField.SNAPSHOT_ID +"}/_update",
                this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.V7_BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots/{" + ModelSnapshotField.SNAPSHOT_ID +"}/_update",
                this);
    }

    @Override
    public String getName() {
        return "xpack_ml_update_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        XContentParser parser = restRequest.contentParser();
        UpdateModelSnapshotAction.Request updateModelSnapshot = UpdateModelSnapshotAction.Request.parseRequest(
                restRequest.param(Job.ID.getPreferredName()),
                restRequest.param(ModelSnapshotField.SNAPSHOT_ID.getPreferredName()),
                parser);

        return channel ->
                client.execute(UpdateModelSnapshotAction.INSTANCE, updateModelSnapshot, new RestStatusToXContentListener<>(channel));
    }
}
