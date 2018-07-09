/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ccr.action.ShardFollowNodeTask;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;

import java.io.IOException;

import static org.elasticsearch.xpack.ccr.action.FollowIndexAction.INSTANCE;
import static org.elasticsearch.xpack.ccr.action.FollowIndexAction.Request;

public class RestFollowIndexAction extends BaseRestHandler {

    public RestFollowIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_xpack/ccr/_follow", this);
    }

    @Override
    public String getName() {
        return "xpack_ccr_follow_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = createRequest(restRequest);
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }

    static Request createRequest(RestRequest restRequest) {
        int maxBatchOperationCount = ShardFollowNodeTask.DEFAULT_MAX_BATCH_OPERATION_COUNT;
        if (restRequest.hasParam(ShardFollowTask.MAX_BATCH_OPERATION_COUNT.getPreferredName())) {
            maxBatchOperationCount = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_BATCH_OPERATION_COUNT.getPreferredName()));
        }
        int maxConcurrentReadBatches = ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_READ_BATCHES;
        if (restRequest.hasParam(ShardFollowTask.MAX_CONCURRENT_READ_BATCHES.getPreferredName())) {
            maxConcurrentReadBatches = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_CONCURRENT_READ_BATCHES.getPreferredName()));
        }
        long maxBatchSizeInBytes = ShardFollowNodeTask.DEFAULT_MAX_BATCH_SIZE_IN_BYTES;
        if (restRequest.hasParam(ShardFollowTask.MAX_BATCH_SIZE_IN_BYTES.getPreferredName())) {
            maxBatchSizeInBytes = Long.valueOf(restRequest.param(ShardFollowTask.MAX_BATCH_SIZE_IN_BYTES.getPreferredName()));
        }
        int maxConcurrentWriteBatches = ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_WRITE_BATCHES;
        if (restRequest.hasParam(ShardFollowTask.MAX_CONCURRENT_WRITE_BATCHES.getPreferredName())) {
            maxConcurrentWriteBatches = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_CONCURRENT_WRITE_BATCHES.getPreferredName()));
        }
        int maxWriteBufferSize = ShardFollowNodeTask.DEFAULT_MAX_WRITE_BUFFER_SIZE;
        if (restRequest.hasParam(ShardFollowTask.MAX_WRITE_BUFFER_SIZE.getPreferredName())) {
            maxWriteBufferSize = Integer.parseInt(restRequest.param(ShardFollowTask.MAX_WRITE_BUFFER_SIZE.getPreferredName()));
        }
        TimeValue retryTimeout = restRequest.paramAsTime(ShardFollowTask.RETRY_TIMEOUT.getPreferredName(),
            ShardFollowNodeTask.DEFAULT_RETRY_TIMEOUT);
        TimeValue idleShardRetryTimeout = restRequest.paramAsTime(ShardFollowTask.IDLE_SHARD_RETRY_DELAY.getPreferredName(),
            ShardFollowNodeTask.DEFAULT_IDLE_SHARD_RETRY_DELAY);
        return new Request(restRequest.param("leader_index"), restRequest.param("index"), maxBatchOperationCount, maxConcurrentReadBatches,
            maxBatchSizeInBytes, maxConcurrentWriteBatches, maxWriteBufferSize, retryTimeout, idleShardRetryTimeout);
    }
}
