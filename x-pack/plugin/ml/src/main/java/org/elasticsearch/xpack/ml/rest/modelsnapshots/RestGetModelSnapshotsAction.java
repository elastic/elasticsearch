/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.modelsnapshots;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction.Request.SNAPSHOT_ID;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestGetModelSnapshotsAction extends BaseRestHandler {

    private static final String ALL_SNAPSHOT_IDS = null;

    // Even though these are null, setting up the defaults in case
    // we want to change them later
    private static final String DEFAULT_SORT = null;
    private static final String DEFAULT_START = null;
    private static final String DEFAULT_END = null;
    private static final boolean DEFAULT_DESC_ORDER = true;

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, BASE_PATH + "anomaly_detectors/{" + ID + "}/model_snapshots/{" + SNAPSHOT_ID + "}"),
            new Route(POST, BASE_PATH + "anomaly_detectors/{" + ID + "}/model_snapshots/{" + SNAPSHOT_ID + "}"),
            new Route(GET, BASE_PATH + "anomaly_detectors/{" + ID + "}/model_snapshots"),
            new Route(POST, BASE_PATH + "anomaly_detectors/{" + ID + "}/model_snapshots")
        );
    }

    @Override
    public String getName() {
        return "ml_get_model_snapshot_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String snapshotId = restRequest.param(Request.SNAPSHOT_ID.getPreferredName());
        if (Strings.isAllOrWildcard(snapshotId)) {
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
            getModelSnapshots.setPageParams(
                new PageParams(
                    restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
                )
            );
        }

        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetModelSnapshotsAction.INSTANCE,
            getModelSnapshots,
            new RestToXContentListener<>(channel)
        );
    }
}
