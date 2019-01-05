/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestIndexUpgradeAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestIndexUpgradeAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestIndexUpgradeAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerWithDeprecatedHandler(
            POST, "_migration/upgrade/{index}", this,
            POST, "_xpack/migration/upgrade/{index}", deprecationLogger);
    }

    @Override
    public String getName() {
        return "migration_upgrade";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method().equals(POST)) {
            return handlePost(request, client);
        } else {
            throw new IllegalArgumentException("illegal method [" + request.method() + "] for request [" + request.path() + "]");
        }
    }

    private RestChannelConsumer handlePost(final RestRequest request, NodeClient client) {
        Request upgradeRequest = new Request(request.param("index"));
        Map<String, String> params = new HashMap<>();
        params.put(BulkByScrollTask.Status.INCLUDE_CREATED, Boolean.toString(true));
        params.put(BulkByScrollTask.Status.INCLUDE_UPDATED, Boolean.toString(true));

        if (request.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.execute(IndexUpgradeAction.INSTANCE, upgradeRequest,
                    new RestBuilderListener<BulkByScrollResponse>(channel) {

                        @Override
                        public RestResponse buildResponse(BulkByScrollResponse response, XContentBuilder builder) throws Exception {
                            builder.startObject();
                            response.toXContent(builder, new ToXContent.DelegatingMapParams(params, channel.request()));
                            builder.endObject();
                            return new BytesRestResponse(getStatus(response), builder);
                        }

                        private RestStatus getStatus(BulkByScrollResponse response) {
                            /*
                             * Return the highest numbered rest status under the assumption that higher numbered statuses are "more error"
                             * and thus more interesting to the user.
                             */
                            RestStatus status = RestStatus.OK;
                            if (response.isTimedOut()) {
                                status = RestStatus.REQUEST_TIMEOUT;
                            }
                            for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
                                if (failure.getStatus().getStatus() > status.getStatus()) {
                                    status = failure.getStatus();
                                }
                            }
                            for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
                                RestStatus failureStatus = ExceptionsHelper.status(failure.getReason());
                                if (failureStatus.getStatus() > status.getStatus()) {
                                    status = failureStatus;
                                }
                            }
                            return status;
                        }

                    });
        } else {
            upgradeRequest.setShouldStoreResult(true);

            /*
             * Validating before forking to make sure we can catch the issues earlier
             */
            ActionRequestValidationException validationException = upgradeRequest.validate();
            if (validationException != null) {
                throw validationException;
            }
            Task task = client.executeLocally(IndexUpgradeAction.INSTANCE, upgradeRequest, LoggingTaskListener.instance());
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

