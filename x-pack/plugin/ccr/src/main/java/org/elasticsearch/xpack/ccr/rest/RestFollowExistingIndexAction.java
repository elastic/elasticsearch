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
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;

import java.io.IOException;

import static org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction.INSTANCE;
import static org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction.Request;

public class RestFollowExistingIndexAction extends BaseRestHandler {

    public RestFollowExistingIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_xpack/ccr/_follow", this);
    }

    @Override
    public String getName() {
        return "xpack_ccr_follow_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = new Request();
        request.setLeaderIndex(restRequest.param("leader_index"));
        request.setFollowIndex(restRequest.param("index"));
        if (restRequest.hasParam(ShardFollowTask.MAX_CHUNK_SIZE.getPreferredName())) {
            request.setBatchSize(Long.valueOf(restRequest.param(ShardFollowTask.MAX_CHUNK_SIZE.getPreferredName())));
        }
        if (restRequest.hasParam(ShardFollowTask.NUM_CONCURRENT_CHUNKS.getPreferredName())) {
            request.setConcurrentProcessors(Integer.valueOf(restRequest.param(ShardFollowTask.NUM_CONCURRENT_CHUNKS.getPreferredName())));
        }
        if (restRequest.hasParam(ShardFollowTask.PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST.getPreferredName())) {
            long value = Long.valueOf(restRequest.param(ShardFollowTask.PROCESSOR_MAX_TRANSLOG_BYTES_PER_REQUEST.getPreferredName()));
            request.setProcessorMaxTranslogBytes(value);
        }
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
