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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction.Request;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestGetModelSnapshotsAction extends BaseRestHandler {

    private final String ALL = "_all";
    private final String ALL_SNAPSHOT_IDS = null;

    // Even though these are null, setting up the defaults in case
    // we want to change them later
    private final String DEFAULT_SORT = null;
    private final String DEFAULT_START = null;
    private final String DEFAULT_END = null;
    private final String DEFAULT_DESCRIPTION = null;
    private final boolean DEFAULT_DESC_ORDER = true;

    public RestGetModelSnapshotsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots/{" + Request.SNAPSHOT_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.V7_BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots/{" + Request.SNAPSHOT_ID.getPreferredName() + "}", this);
        // endpoints that support body parameters must also accept POST
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots/{" + Request.SNAPSHOT_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.V7_BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots/{" + Request.SNAPSHOT_ID.getPreferredName() + "}", this);

        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.V7_BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots", this);
        // endpoints that support body parameters must also accept POST
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots", this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.V7_BASE_PATH + "anomaly_detectors/{"
                + Job.ID.getPreferredName() + "}/model_snapshots", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_get_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String snapshotId = restRequest.param(Request.SNAPSHOT_ID.getPreferredName());
        if (ALL.equals(snapshotId)) {
            snapshotId = ALL_SNAPSHOT_IDS;
        }
        Request getModelSnapshots;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            getModelSnapshots = Request.parseRequest(jobId, snapshotId, parser);
        } else {
            getModelSnapshots = new Request(jobId, snapshotId);
            getModelSnapshots.setSort(restRequest.param(Request.SORT.getPreferredName(), DEFAULT_SORT));
            if (restRequest.hasParam(Request.START.getPreferredName())) {
                getModelSnapshots.setStart(restRequest.param(Request.START.getPreferredName(), DEFAULT_START));
            }
            if (restRequest.hasParam(Request.END.getPreferredName())) {
                getModelSnapshots.setEnd(restRequest.param(Request.END.getPreferredName(), DEFAULT_END));
            }
            getModelSnapshots.setDescOrder(restRequest.paramAsBoolean(Request.DESC.getPreferredName(), DEFAULT_DESC_ORDER));
            getModelSnapshots.setPageParams(new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }

        return channel -> client.execute(GetModelSnapshotsAction.INSTANCE, getModelSnapshots, new RestToXContentListener<>(channel));
    }
}
