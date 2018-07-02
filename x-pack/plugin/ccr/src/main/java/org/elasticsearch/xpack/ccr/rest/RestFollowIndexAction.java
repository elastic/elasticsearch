/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
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
        int maxOperationCount = ShardFollowNodeTask.DEFAULT_MAX_OPERATION_COUNT;
        if (restRequest.hasParam(ShardFollowTask.MAX_OPERATION_COUNT.getPreferredName())) {
            maxOperationCount = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_OPERATION_COUNT.getPreferredName()));
        }
        int maxConcurrentReads = ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_READS;
        if (restRequest.hasParam(ShardFollowTask.MAX_CONCURRENT_READS.getPreferredName())) {
            maxConcurrentReads = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_CONCURRENT_READS.getPreferredName()));
        }
        long maxOperationSizeInBytes = ShardFollowNodeTask.DEFAULT_MAX_OPERATIONS_SIZE_IN_BYTES;
        if (restRequest.hasParam(ShardFollowTask.MAX_OPERATION_SIZE_IN_BYTES.getPreferredName())) {
            maxOperationSizeInBytes = Long.valueOf(restRequest.param(ShardFollowTask.MAX_OPERATION_SIZE_IN_BYTES.getPreferredName()));
        }
        int maxWriteSize = ShardFollowNodeTask.DEFAULT_MAX_WRITE_SIZE;
        if (restRequest.hasParam(ShardFollowTask.MAX_WRITE_SIZE.getPreferredName())) {
            maxWriteSize = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_WRITE_SIZE.getPreferredName()));
        }
        int maxConcurrentWrites = ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_WRITES;
        if (restRequest.hasParam(ShardFollowTask.MAX_CONCURRENT_WRITES.getPreferredName())) {
            maxConcurrentWrites = Integer.valueOf(restRequest.param(ShardFollowTask.MAX_CONCURRENT_WRITES.getPreferredName()));
        }
        int maxBufferSize = ShardFollowNodeTask.DEFAULT_MAX_BUFFER_SIZE;
        if (restRequest.hasParam(ShardFollowTask.MAX_BUFFER_SIZE.getPreferredName())) {
            maxBufferSize = Integer.parseInt(restRequest.param(ShardFollowTask.MAX_BUFFER_SIZE.getPreferredName()));
        }
        return new Request(restRequest.param("leader_index"), restRequest.param("index"), maxOperationCount, maxConcurrentReads,
            maxOperationSizeInBytes, maxWriteSize, maxConcurrentWrites, maxBufferSize);
    }
}
