/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.results;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.action.ResultsIndexUpgradeAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpgradeResultsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestUpgradeResultsAction.class));

    public RestUpgradeResultsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerWithDeprecatedHandler(
            POST,
            MachineLearning.BASE_PATH + "anomaly_detectors/results/_upgrade",
            this,
            POST,
            MachineLearning.PRE_V7_BASE_PATH + "anomaly_detectors/results/_upgrade",
            deprecationLogger);
    }

    @Override
    public String getName() {
        return "xpack_ml_upgrade_results_indices_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        ResultsIndexUpgradeAction.Request parsedRequest = new ResultsIndexUpgradeAction.Request();
        if (restRequest.hasContent()) {
            XContentParser parser = restRequest.contentParser();
            parsedRequest = ResultsIndexUpgradeAction.Request.PARSER.apply(parser, null);
        }
        final ResultsIndexUpgradeAction.Request upgradeRequest = parsedRequest;

        if (restRequest.paramAsBoolean("wait_for_completion", false)) {
            return channel -> client.execute(ResultsIndexUpgradeAction.INSTANCE, upgradeRequest, new RestToXContentListener<>(channel));
        } else {
            upgradeRequest.setShouldStoreResult(true);

            Task task = client.executeLocally(ResultsIndexUpgradeAction.INSTANCE, upgradeRequest,  LoggingTaskListener.instance());
            // Send task description id instead of waiting for the message
            return channel -> {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("task", client.getLocalNodeId() + ":" + task.getId());
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            };
        }
    }
}
